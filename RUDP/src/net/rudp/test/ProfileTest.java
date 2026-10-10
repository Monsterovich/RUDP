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
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp.test;

import net.rudp.ReliableSocketProfile;

/**
 * Tests the bounds a socket profile enforces on its own parameters and the
 * clamping a peer's proposal is brought into.
 * <p>
 * The constructor is the only place a local misconfiguration is caught: a
 * window or a queue larger than the 8-bit sequence space can order does not
 * fail at the point it is set, it fails later as segments that are silently
 * treated as duplicates or dropped as out of window. Rejecting the value up
 * front is the difference between a configuration error and a connection that
 * misbehaves for no visible reason.
 * <p>
 * bounded() is the other side of the same coin: the maximum outstanding is
 * also a field of the handshake, so it arrives from the peer, and the value
 * has to be brought into the usable range rather than rejected - the two ends
 * have already agreed to talk.
 */
public class ProfileTest
{
    /* The eleven parameters, in the order the constructor takes them. */
    private static final int SEND_QUEUE     = 0;
    private static final int RECV_QUEUE     = 1;
    private static final int SEGMENT_SIZE   = 2;
    private static final int OUTSTANDING    = 3;
    private static final int RETRANS        = 4;
    private static final int CUMULATIVE     = 5;
    private static final int OUT_OF_SEQ     = 6;
    private static final int AUTO_RESET     = 7;
    private static final int NULL_TIMEOUT   = 8;
    private static final int RETX_TIMEOUT   = 9;
    private static final int CUMACK_TIMEOUT = 10;

    public static void main(String[] args)
    {
        Assert.suite("socket profile");

        testDefaults();
        testGetters();
        testBounds();
        testRejectsOutOfRange();
        testBounded();
        testToString();

        System.exit(Assert.report());
    }

    private static void testDefaults()
    {
        Assert.test("the default profile uses the published defaults", () -> {
            ReliableSocketProfile p = new ReliableSocketProfile();

            Assert.equals("maxSendQueueSize", ReliableSocketProfile.MAX_SEND_QUEUE_SIZE,
                    p.maxSendQueueSize());
            Assert.equals("maxRecvQueueSize", ReliableSocketProfile.MAX_RECV_QUEUE_SIZE,
                    p.maxRecvQueueSize());
            Assert.equals("maxSegmentSize", ReliableSocketProfile.MAX_SEGMENT_SIZE,
                    p.maxSegmentSize());
            Assert.equals("maxOutstandingSegs", ReliableSocketProfile.MAX_OUTSTANDING_SEGS,
                    p.maxOutstandingSegs());

            /*
             * The default retransmission limit is zero, not MAX_RETRANS: the
             * draft's default of 3 gives up on a lossy path, so the library
             * chooses unlimited instead. See the constructor's comment.
             */
            Assert.equals("maxRetrans", 0, p.maxRetrans());

            Assert.equals("maxCumulativeAcks", ReliableSocketProfile.MAX_CUMULATIVE_ACKS,
                    p.maxCumulativeAcks());
            Assert.equals("maxOutOfSequence", ReliableSocketProfile.MAX_OUT_OF_SEQUENCE,
                    p.maxOutOfSequence());
            Assert.equals("maxAutoReset", ReliableSocketProfile.MAX_AUTO_RESET,
                    p.maxAutoReset());
            Assert.equals("nullSegmentTimeout", ReliableSocketProfile.NULL_SEGMENT_TIMEOUT,
                    p.nullSegmentTimeout());
            Assert.equals("retransmissionTimeout",
                    ReliableSocketProfile.RETRANSMISSION_TIMEOUT, p.retransmissionTimeout());
            Assert.equals("cumulativeAckTimeout",
                    ReliableSocketProfile.CUMULATIVE_ACK_TIMEOUT, p.cumulativeAckTimeout());
        });
    }

    private static void testGetters()
    {
        Assert.test("returns every value it was built with", () -> {
            ReliableSocketProfile p = new ReliableSocketProfile(
                    90, 80, 1200, 70, 5, 6, 7, 8, 2100, 250, 350);

            Assert.equals("maxSendQueueSize", 90, p.maxSendQueueSize());
            Assert.equals("maxRecvQueueSize", 80, p.maxRecvQueueSize());
            Assert.equals("maxSegmentSize", 1200, p.maxSegmentSize());
            Assert.equals("maxOutstandingSegs", 70, p.maxOutstandingSegs());
            Assert.equals("maxRetrans", 5, p.maxRetrans());
            Assert.equals("maxCumulativeAcks", 6, p.maxCumulativeAcks());
            Assert.equals("maxOutOfSequence", 7, p.maxOutOfSequence());
            Assert.equals("maxAutoReset", 8, p.maxAutoReset());
            Assert.equals("nullSegmentTimeout", 2100, p.nullSegmentTimeout());
            Assert.equals("retransmissionTimeout", 250, p.retransmissionTimeout());
            Assert.equals("cumulativeAckTimeout", 350, p.cumulativeAckTimeout());
        });
    }

    private static void testBounds()
    {
        Assert.test("accepts each parameter at both ends of its range", () -> {
            int[] mins = { 1, 1, 22, 1, 0, 0, 0, 0, 0, 100, 100 };
            int[] maxs = { ReliableSocketProfile.MAX_WINDOW_SEGS,
                    ReliableSocketProfile.MAX_WINDOW_SEGS, 65535,
                    ReliableSocketProfile.MAX_WINDOW_SEGS, 255, 255, 255, 255,
                    65535, 65535, 65535 };

            for (int i = 0; i < mins.length; i++) {
                make(with(valid(), i, mins[i]));
                make(with(valid(), i, maxs[i]));
            }
        });

        Assert.test("the largest window a profile may set is the half-space bound", () -> {
            /*
             * A window of 128 already puts a segment a number of positions too
             * far ahead to be told apart from a duplicate, which is why the
             * bound is 127 rather than 128. The profile is where that is
             * enforced.
             */
            Assert.equals("bound", 127, ReliableSocketProfile.MAX_WINDOW_SEGS);

            make(with(valid(), OUTSTANDING, 127));
        });
    }

    private static void testRejectsOutOfRange()
    {
        Assert.test("rejects each parameter one step outside its range", () -> {
            int[] mins = { 1, 1, 22, 1, 0, 0, 0, 0, 0, 100, 100 };
            int[] maxs = { ReliableSocketProfile.MAX_WINDOW_SEGS,
                    ReliableSocketProfile.MAX_WINDOW_SEGS, 65535,
                    ReliableSocketProfile.MAX_WINDOW_SEGS, 255, 255, 255, 255,
                    65535, 65535, 65535 };
            String[] names = { "maxSendQueueSize", "maxRecvQueueSize", "maxSegmentSize",
                    "maxOutstandingSegs", "maxRetrans", "maxCumulativeAcks",
                    "maxOutOfSequence", "maxAutoReset", "nullSegmentTimeout",
                    "retransmissionTimeout", "cumulativeAckTimeout" };

            for (int i = 0; i < mins.length; i++) {
                rejects(names[i], with(valid(), i, mins[i] - 1));
                rejects(names[i], with(valid(), i, maxs[i] + 1));
            }
        });

        Assert.test("names the parameter it rejected", () -> {
            Assert.throwsExactly("the message is the parameter name",
                    IllegalArgumentException.class, () -> {
                        new ReliableSocketProfile(
                                64, 64, 21 /* too small */, 32, 3, 3, 3, 3, 2000, 200, 300);
                    });

            try {
                new ReliableSocketProfile(
                        64, 64, 21, 32, 3, 3, 3, 3, 2000, 200, 300);
                throw new AssertionError("the constructor accepted 21");
            }
            catch (IllegalArgumentException xcp) {
                Assert.equals("message", "maxSegmentSize", xcp.getMessage());
            }
        });
    }

    private static void testBounded()
    {
        Assert.test("brings a peer's proposal into the usable range", () -> {
            Assert.equals("a negative proposal", 1, ReliableSocketProfile.bounded(-5));
            Assert.equals("zero", 1, ReliableSocketProfile.bounded(0));
            Assert.equals("one", 1, ReliableSocketProfile.bounded(1));
            Assert.equals("an ordinary window", 64, ReliableSocketProfile.bounded(64));
            Assert.equals("the bound exactly", 127, ReliableSocketProfile.bounded(127));
        });

        Assert.test("clamps a proposal past the half-space down to it", () -> {
            Assert.equals("128", ReliableSocketProfile.MAX_WINDOW_SEGS,
                    ReliableSocketProfile.bounded(128));
            Assert.equals("a window from a future version", ReliableSocketProfile.MAX_WINDOW_SEGS,
                    ReliableSocketProfile.bounded(1000));
        });
    }

    private static void testToString()
    {
        Assert.test("prints every value in constructor order", () -> {
            ReliableSocketProfile p = new ReliableSocketProfile(
                    1, 2, 1200, 4, 5, 6, 7, 8, 9, 100, 101);

            Assert.equals("toString", "[1, 2, 1200, 4, 5, 6, 7, 8, 9, 100, 101]",
                    p.toString());
        });
    }

    /** The eleven values, all inside their ranges and all distinct. */
    private static int[] valid()
    {
        return new int[] { 64, 64, 1200, 32, 3, 3, 3, 3, 2000, 200, 300 };
    }

    private static int[] with(int[] base, int index, int value)
    {
        int[] copy = base.clone();
        copy[index] = value;
        return copy;
    }

    private static ReliableSocketProfile make(int[] v)
    {
        return new ReliableSocketProfile(v[SEND_QUEUE], v[RECV_QUEUE], v[SEGMENT_SIZE],
                v[OUTSTANDING], v[RETRANS], v[CUMULATIVE], v[OUT_OF_SEQ], v[AUTO_RESET],
                v[NULL_TIMEOUT], v[RETX_TIMEOUT], v[CUMACK_TIMEOUT]);
    }

    private static void rejects(String name, int[] v)
    {
        Assert.throwsExactly(name + " rejected", IllegalArgumentException.class, () -> make(v));
    }
}
