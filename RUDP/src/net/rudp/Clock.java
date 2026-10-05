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

package net.rudp;

/**
 * Source of the time used by a socket to stamp transmission instants, to
 * compute retransmission deadlines and to enforce timeouts.
 * <p>
 * Every timestamp a socket takes goes through this interface, so a test can
 * substitute a clock it advances by hand and decide exactly when a timeout
 * becomes due. Without that seam the retransmission logic can only be
 * exercised by sleeping, which makes a regression in the backoff schedule
 * indistinguishable from a slow machine.
 * <p>
 * Implementations are expected to be monotonic: the socket only ever takes
 * differences, and a reading that goes backwards turns into a round trip
 * sample of negative length or into a deadline that never comes due.
 */
public interface Clock
{
    /**
     * Returns the current time in milliseconds. Only differences between two
     * returned values are meaningful; the origin is arbitrary.
     *
     * @return the current time in milliseconds.
     */
    long currentTimeMillis();

    /**
     * The real system clock, used by every socket unless one is injected.
     * <p>
     * Backed by System.nanoTime() rather than System.currentTimeMillis().
     * The two differ in a way this clock cannot afford: the wall clock is
     * adjusted, and a socket measures intervals across those adjustments.
     * A step forward - an NTP correction, an operator setting the clock by
     * hand - lands in a round trip sample as a large positive value and
     * carries the RTO to its ceiling for as long as the smoothed estimate
     * takes to come back down, so one clock correction stalls the connection
     * for seconds. A step back - the hour a daylight saving transition takes
     * away, or an NTP correction the other way - lands in the same sample as
     * a negative value, and lands in every deadline already handed out as a
     * subtraction that stays positive long after it should have expired, so
     * nothing is retransmitted until the wall clock catches up.
     * <p>
     * System.nanoTime() has neither failure mode: its origin is arbitrary
     * and undocumented but its differences are exact, which is all that is
     * asked of it here. The division to milliseconds is the only cost, and
     * it keeps the interface's unit.
     */
    Clock SYSTEM = new Clock() {
        public long currentTimeMillis()
        {
            return System.nanoTime() / 1000000L;
        }

        public String toString()
        {
            return "Clock.SYSTEM";
        }
    };
}
