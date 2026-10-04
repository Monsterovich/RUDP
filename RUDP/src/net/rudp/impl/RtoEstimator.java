/*
 * Simple Reliable UDP (rudp)
 * Copyright (c) 2026, Nikolay Borodin (monsterovich@gmail.com)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp.impl;

/**
 * The retransmission timeout policy of a connection (RFC 6298): a smoothed
 * round trip time estimate folded into an RTO, plus the exponential backoff a
 * segment accumulates when it is retransmitted on a timeout.
 * <p>
 * This is deliberately a pure object with no clock and no threads: it is fed
 * round trip samples and backoff shifts and answers with a timeout, so the
 * schedule can be asserted exactly instead of being observed through sleeps.
 * Not thread safe; a socket confines it to its retransmission monitor.
 */
public final class RtoEstimator
{
    /** Floor of the computed RTO, in milliseconds. */
    public static final long MIN_RTO = 50;

    /** Ceiling of any RTO handed out, in milliseconds. */
    public static final long MAX_RTO = 5000;

    /**
     * Largest number of exponential backoff doublings a segment may
     * accumulate, i.e. a timed out segment waits at most 16 times the
     * current RTO (and at most MAX_RTO).
     */
    public static final int MAX_BACKOFF_SHIFT = 4;

    /**
     * Creates an estimator that has not taken a round trip sample yet and
     * therefore uses the given profile timeout as its initial RTO.
     *
     * @param initialRtoMillis the profile's retransmission timeout.
     */
    public RtoEstimator(int initialRtoMillis)
    {
        if (initialRtoMillis < 1) {
            throw new IllegalArgumentException("initialRtoMillis must be positive");
        }

        _rto = initialRtoMillis;
    }

    /**
     * Folds one round trip time sample into the smoothed estimators and
     * recomputes the RTO.
     *
     * @param rttMillis the measured round trip time, in milliseconds.
     */
    public void updateSample(long rttMillis)
    {
        if (rttMillis < 1) {
            rttMillis = 1;
        }

        if (_srtt < 0) {
            _srtt = rttMillis;
            _rttvar = rttMillis / 2;
        }
        else {
            long delta = Math.abs(_srtt - rttMillis);
            _rttvar = (3 * _rttvar + delta) / 4;
            _srtt = (7 * _srtt + rttMillis) / 8;
        }

        long rto = _srtt + 4 * _rttvar;
        if (rto < MIN_RTO) {
            rto = MIN_RTO;
        }
        else if (rto > MAX_RTO) {
            rto = MAX_RTO;
        }

        _rto = (int) rto;
    }

    /**
     * Returns the current RTO, without any backoff applied.
     *
     * @return the retransmission timeout in milliseconds.
     */
    public int rto()
    {
        return _rto;
    }

    /**
     * Returns the timeout to stamp on a segment that has accumulated the
     * given number of exponential backoff doublings.
     *
     * @param backoffShift the segment's backoff doublings.
     * @return the effective timeout in milliseconds.
     */
    public int rtoFor(int backoffShift)
    {
        if (backoffShift < 0) {
            throw new IllegalArgumentException("backoffShift must not be negative");
        }

        long rto = ((long) _rto) << backoffShift;
        return (int) (rto > MAX_RTO ? MAX_RTO : rto);
    }

    /**
     * Returns whether at least one round trip sample has been taken. Until
     * then the RTO is the profile default rather than a measured value.
     *
     * @return true if a sample has been folded in.
     */
    public boolean hasSample()
    {
        return _srtt >= 0;
    }

    /**
     * Returns the smoothed round trip time estimate, or -1 if no sample has
     * been taken yet.
     *
     * @return the smoothed round trip time in milliseconds.
     */
    public long srtt()
    {
        return _srtt;
    }

    /**
     * Returns the round trip time variation estimate.
     *
     * @return the variation in milliseconds.
     */
    public long rttvar()
    {
        return _rttvar;
    }

    private long _srtt = -1;
    private long _rttvar = 0;
    private int _rto;
}
