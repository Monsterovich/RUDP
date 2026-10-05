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
 *     * Neither the name of the copyright nor the names of its
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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.rudp.Clock;
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketProfile;
import net.rudp.impl.ACKSegment;
import net.rudp.impl.DATSegment;
import net.rudp.impl.EAKSegment;
import net.rudp.impl.Segment;

/**
 * Tests the congestion window of a live connection, acknowledged segment by
 * segment by a peer the test drives itself.
 * <p>
 * The client is connected to a real server over loopback so the handshake and
 * the queues are genuine, and then the server's port is silenced on the
 * client's socket and an injector takes its place. Nothing else can be done
 * here: the window is not a number anything reports, it is a number of
 * segments the sender is allowed to have in flight, so the only way to read it
 * is to send until the socket stops and count what went out. A real peer
 * acknowledging at its own pace would leave that count to a race between the
 * test and the clock, and every wrong answer it produced would look exactly
 * like a right one.
 * <p>
 * Two conventions carry most of the file.
 * <ul>
 * <li>Windows are counted from an empty queue. An acknowledgment takes a
 *     segment off the queue and frees its slot, so after one the sender is
 *     entitled to exactly as many segments as were acknowledged, whatever the
 *     window is - only a queue that has been emptied leaves the writer's own
 *     doing as the whole of what it was allowed to send.</li>
 * <li>Growth is counted one acknowledgment at a time. Each one frees one
 *     slot, and a window that grew by one on that acknowledgment frees two,
 *     so the number of segments admitted after each is the growth, seen
 *     against a denominator of one segment per round trip.</li>
 * </ul>
 */
public class CongestionControlTest
{
    /**
     * Long enough for the handshake, short enough to bound a broken run.
     * <p>
     * As in the retransmission tests, connect() compares this against the
     * socket's own clock, which is frozen, so the wait is real time while the
     * comparison never becomes true. A handshake that stalls surfaces as a
     * connection failure rather than as a SocketTimeoutException, and the
     * cases below assert nothing about which.
     */
    private static final int CONNECT_TIMEOUT_MS = 10000;

    /** One step of the clock walk; small enough to locate a deadline exactly. */
    private static final int STEP_MS = 1;

    /**
     * How long to wait for the reader thread to have dealt with an injected
     * segment, or for the writer to have reached the wire. Real time, and used
     * only for thread scheduling: nothing below is stated in terms of it.
     */
    private static final int INJECTION_TIMEOUT_MS = 5000;

    /** How long a wire is watched to conclude that nothing more is coming. */
    private static final int QUIET_GRACE_MS = 250;

    /**
     * Enough manual time to outlast any deadline a segment can be given: the
     * backoff is capped at MAX_BACKOFF_SHIFT doublings of a timeout that is
     * itself capped, so nothing stays due beyond this.
     */
    private static final long FAR_FUTURE_MS = 10L * 60L * 1000L;

    /** One written segment. Small enough that a write is exactly one of them. */
    private static final byte[] SEGMENT_PAYLOAD = { 1, 2, 3, 4, 5 };

    /** Duplicate acknowledgments before the sender assumes a loss (RFC 5681 3.2). */
    private static final int DUP_ACKS = 3;

    public static void main(String[] args)
    {
        Assert.suite("congestion control");

        testSlowStart();
        testCongestionAvoidance();
        testLossResponse();
        testDuplicateAcks();

        System.exit(Assert.report());
    }

    /* ------------------------------------------------------------------
     * Slow start: doubling, the ceiling, and the threshold that ends it.
     * ------------------------------------------------------------------ */

    private static void testSlowStart()
    {
        Assert.test("the window doubles in slow start and stops at the ceiling", () -> {
            Link link = Link.open(profile(64));

            try {
                Assert.equals("the window a connection starts with", 10, link.writer().fill());

                /*
                 * The ceiling is what the case is for. A window the peer has
                 * already capped at maxOutstandingSegs cannot buy anything by
                 * growing past it - the smaller of the two is what the sender is
                 * held to - so a slow start with nothing to stop it ends up
                 * carrying a number in the hundreds whose only remaining job is to
                 * be halved at the next loss, and half of it is still past the
                 * ceiling, so the halving undoes itself in a single step.
                 */
                assertWindow(link, "the window after one round of acknowledgments", 20);
                assertWindow(link, "the window after two", 40);
                assertWindow(link, "the window after three, at the ceiling", 64);
                assertWindow(link, "and after one more round it is still at the ceiling", 64);
            }
            finally {
                link.close();
            }
        });

        Assert.test("a window that timed out together is cut once, and stops doubling "
                + "once it reaches the threshold", () -> {
            Link link = Link.open(profile(200));

            try {
                int inFlight = link.writer().fill();
                Assert.equals("the initial window", 10, inFlight);

                /*
                 * Every segment of a window that went down together was sent at the
                 * same instant against a frozen clock, so they share one deadline
                 * and the whole window comes due in a single pass.
                 *
                 * Unlimited retransmissions, so that the pass below is not also the
                 * last one: what is under test is the state it leaves the window
                 * in, and a connection torn down cannot be inspected.
                 */
                link.advanceToTimeout();
                link.runSchedule();

                Assert.equals("the whole window came due at once",
                        inFlight, link.wire().dataCount() - inFlight);

                /*
                 * The threshold is half the flight - five segments, not the two
                 * that halving the window once per expired segment leaves. A window
                 * of one segment with a single acknowledgment on top of it still
                 * reaches five, so the very next acknowledgment takes the sender
                 * out of slow start. A threshold that nothing ever consults leaves
                 * it doubling instead, which is the whole difference between a
                 * window that has found the path's capacity and one that never
                 * looks for it.
                 */
                Assert.equals("the window one acknowledgment after the timeout", 11, link.readWindow());
                Assert.equals("and one round trip later it has grown by one", 12, link.readWindow());
            }
            finally {
                link.close();
            }
        });
    }

    /* ------------------------------------------------------------------
     * Congestion avoidance: one segment per round trip.
     * ------------------------------------------------------------------ */

    private static void testCongestionAvoidance()
    {
        Assert.test("congestion avoidance adds one segment per round trip, not per ack", () -> {
            Link link = Link.open(profile(200));

            try {
                int inFlight = link.writer().fill();

                Assert.equals("the window a connection starts with", 10, inFlight);

                /*
                 * Losing a segment is what moves a sender out of slow start, and
                 * the cheapest way to lose one is for the peer to say it has not
                 * received it: three acknowledgments of a number it has already
                 * been given, which is the signal RFC 5681 3.2 defines and the
                 * only one the client acts on. Four of them are sent because the
                 * first establishes the number to repeat and is not a duplicate of
                 * anything; the three after it are.
                 *
                 * The number is the anchor - the handshake segment's, which took
                 * nothing off the queue - so what the duplicates are complaining
                 * about is the segment behind it, the first thing the client ever
                 * wrote. Acknowledging anything newer would take the head of the
                 * queue with it and leave nothing to retransmit.
                 */
                long lost = link.anchorSeq();

                link.injector().ack(lost);
                for (int i = 0; i < DUP_ACKS; i++) {
                    link.injector().ack(lost);
                }

                link.awaitDataSegments(inFlight + 1);

                /*
                 * Half the flight, which is five of the ten still out - not one.
                 * A sender put back to a single segment has to wait a round trip
                 * for the one it may send and another to hear about it, which on a
                 * path that dropped one segment in ten costs far more than the
                 * loss did. The rest of the flight is ten segments in one
                 * acknowledgment, so two increments' worth come out of it and
                 * five of them are still owed: the window is six, and a window of
                 * six with five owed in it is a window that has not finished the
                 * round trip it has already been paid for.
                 */
                Assert.equals("the window avoidance opened from", 6, link.readWindow());

                /*
                 * One segment acknowledged at a time, which is what the
                 * denominators have to be stated in. The window grows by one only
                 * once a window's worth of acknowledgments has come in, so the
                 * debt the drain left behind pays for the next one - it admits two,
                 * one for the slot the acknowledgment freed and one for the
                 * segment the window has just earned - and the five after it
                 * admit one each, the debt being spent and nothing owed until the
                 * sixth of them.
                 *
                 * Counting acknowledgments that advance the ack point instead would
                 * admit two for every one of them, which is what congestion
                 * avoidance used to amount to: on a path that acknowledges each
                 * segment as it arrives, that is slow start with the label
                 * changed.
                 */
                int[] admitted = { 2, 1, 1, 1, 1, 1 };

                for (int i = 0; i < admitted.length; i++) {
                    assertAdmitted(link, "acknowledgment " + (i + 1), admitted[i]);
                }
            }
            finally {
                link.close();
            }
        });
    }

    /* ------------------------------------------------------------------
     * The response to a loss: once, and about half the flight.
     * ------------------------------------------------------------------ */

    private static void testLossResponse()
    {
        Assert.test("a loss reported four times cuts the window once", () -> {
            Link link = Link.open(profile(200));

            try {
                int inFlight = link.writer().fill();

                /*
                 * The peer is told it never received the first of the window and
                 * has received everything after it, which is the shape an extended
                 * acknowledgment exists to describe: one segment lost, the rest of
                 * the window delivered.
                 */
                long hole = link.firstUnackedSeq();
                long afterHole = link.lastUnackedSeq();

                link.injector().eak(hole, afterHole);

                /*
                 * Two segments, and not one. The report fills the hole, so the
                 * segment the peer says it never received goes back on the wire
                 * at once; and it also takes the first segment of the window off
                 * the schedule, which frees a slot the writer has been parked
                 * on, so the write that was waiting goes through. Both are what
                 * the window is supposed to allow - a window counts what is in
                 * flight, so an acknowledgment that takes one off the schedule
                 * admits one more - and the second is also why the window the
                 * case goes on to read is read against the flight the report
                 * leaves behind rather than the one it found.
                 */
                link.awaitDataSegments(inFlight + 2);

                /*
                 * Four more reports of the same hole, which is what a real peer
                 * sends - one per few out-of-order arrivals, and the hole is still
                 * there each time because nothing has filled it yet. Refilling it
                 * for each is throttled; what must not happen either is the window
                 * being cut again, on the strength of one lost segment reported
                 * five times over.
                 */
                for (int i = 0; i < 4; i++) {
                    link.injector().eak(hole, afterHole);
                }

                Assert.equals("nothing but the first report reached the wire",
                        inFlight + 2, link.wire().dataCount());

                pause(QUIET_GRACE_MS);

                Assert.equals("and nothing followed it",
                        inFlight + 2, link.wire().dataCount());

                /*
                 * Half the eight the report left in flight, and one more for the
                 * nine segments acknowledging them is worth: nine acknowledged
                 * segments against a window of four are two round trips' worth
                 * and one of them spent. Cut once per report instead, the four
                 * above would take the window down to the floor.
                 */
                Assert.equals("the window after five reports of one loss", 6, link.readWindow());
            }
            finally {
                link.close();
            }
        });

        Assert.test("a window grown past its own ceiling still comes down after a loss", () -> {
            Link link = Link.open(profile(64));

            try {
                /*
                 * Three rounds of acknowledgments, so that a window with no
                 * ceiling of its own would stand at more than twice what the peer
                 * is willing to hold. Only the number of segments in flight can be
                 * observed, and that is the whole of what such a window can be
                 * caught doing before the path loses something: it is
                 * indistinguishable from a well behaved one until then, and the
                 * response is measured against a window the peer never agreed to in
                 * the first place.
                 */
                Assert.equals("the window a connection starts with", 10, link.writer().fill());
                assertWindow(link, "after one round", 20);
                assertWindow(link, "after two", 40);
                assertWindow(link, "after three, at the ceiling", 64);

                long hole = link.firstUnackedSeq();
                long afterHole = link.lastUnackedSeq();

                link.injector().eak(hole, afterHole);
                link.awaitDataSegments(link.wire().dataCount() + 2);

                int after = link.readWindow();

                Assert.isTrue("the window came down (" + after + " of 64)", after < 64);
                Assert.isTrue("and by about half rather than less (" + after + ")", after >= 16);
            }
            finally {
                link.close();
            }
        });
    }

    /* ------------------------------------------------------------------
     * What counts as a duplicate acknowledgment.
     * ------------------------------------------------------------------ */

    private static void testDuplicateAcks()
    {
        Assert.test("only a bare acknowledgment counts as a duplicate one", () -> {
            Link link = Link.open(profile(200));

            try {
                link.writer().fill();

                int quiet = link.wire().dataCount();

                /*
                 * One acknowledgment of the anchor, which took nothing off
                 * the queue, and then two runs of segments from the peer that each
                 * carry that same acknowledgment and nothing newer: a run of its
                 * own data segments, which is how a peer with traffic to send
                 * acknowledges what it has received, and a run of extended
                 * acknowledgments, which are not acknowledgments at all but
                 * reports of a hole.
                 *
                 * The runs are consecutive on purpose. Interleaved with bare
                 * acknowledgments they would reset the count at every bare one and
                 * a sender that counted them would never see three in a row either,
                 * so the case would pass for the wrong reason - what is under test
                 * is the kind of segment, not the order the peer happens to send
                 * them in.
                 */
                long acked = link.anchorSeq();

                link.injector().ack(acked);

                for (int i = 0; i < DUP_ACKS + 1; i++) {
                    link.injector().dat(i, acked);
                }

                for (int i = 0; i < DUP_ACKS + 1; i++) {
                    link.injector().eak(acked, acked);
                }

                /*
                 * Nothing may go out. Every one of those segments says the client
                 * has been told nothing new since the last time, which is a
                 * duplicate acknowledgment - but none of them is an acknowledgment.
                 * Read as one, the third segment of either run puts the head of a
                 * window the peer is receiving perfectly well straight back on the
                 * wire and halves the window over a link that has not lost
                 * anything.
                 */
                pause(QUIET_GRACE_MS);

                Assert.equals("the peer's own traffic put nothing back on the wire",
                        quiet, link.wire().dataCount());

                /*
                 * And the window is untouched: a slow start that has had its whole
                 * window acknowledged has a window of twenty. A loss invented out
                 * of the peer's own segments would have taken it to the threshold
                 * instead, which is half the flight.
                 */
                assertWindow(link, "the window after a round of acknowledgments", 20);
            }
            finally {
                link.close();
            }
        });
    }

    /* ------------------------------------------------------------------
     * Reading the window.
     * ------------------------------------------------------------------ */

    /**
     * Acknowledges everything outstanding - which empties the queue - and
     * requires that the writer was then let send exactly the given number of
     * segments.
     */
    private static void assertWindow(Link link, String what, int expected)
    {
        Assert.equals(what, expected, link.readWindow());
    }

    /**
     * Acknowledges one segment and requires that exactly the given number of
     * segments got through as a result.
     */
    private static void assertAdmitted(Link link, String what, int expected)
    {
        link.writer().mark();
        link.injector().ack(link.nextUnackedSeq());

        Assert.equals(what + " admitted", expected, link.writer().fill());
    }

    /**
     * A profile with the largest queues the sequence number space can order
     * and no limit on retransmissions.
     * <p>
     * The queues are as deep as they may be so that the window and not the
     * queue is what holds the writer back - the socket checks the two in the
     * same line, and a case about congestion control that was really about
     * queue sizing would prove nothing about congestion control. The
     * retransmissions are unlimited so that the schedule is only ever driven
     * by this test.
     * <p>
     * A ceiling above what the 8-bit sequence number space can order is
     * clamped rather than rejected, because a ceiling is only ever read as
     * min(maxOutstandingSegs, cwnd) and the congestion window never grows
     * anywhere near the space: the cases below ask for one to stand in for
     * "no ceiling at all", which the window being the smaller of the two
     * still gives them for every value this test produces.
     */
    private static ReliableSocketProfile profile(int maxOutstanding)
    {
        int ceiling = Math.min(maxOutstanding, ReliableSocketProfile.MAX_WINDOW_SEGS);

        return new ReliableSocketProfile(ReliableSocketProfile.MAX_WINDOW_SEGS,
                ReliableSocketProfile.MAX_WINDOW_SEGS, 1200, ceiling, 0, 3, 3, 3,
                60000, 200, 300);
    }

    /**
     * A pause the test's own threads take while waiting for another thread to
     * reach a state. Named pause() rather than sleep() because Writer extends
     * Thread and would otherwise resolve this to Thread.sleep, which throws
     * where this one does not.
     */
    private static void pause(int millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException xcp) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting");
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException xcp) {
            /* the test is over either way */
        }
    }

    /**
     * A connected client, the peer it is talking to, and the seams both are
     * reached through, so that a case reads as a sequence of acknowledgments
     * and window readings and nothing else.
     */
    private static final class Link
    {
        static Link open(ReliableSocketProfile profile)
            throws IOException, InterruptedException
        {
            Link link = new Link();

            link._server = link._peer.start();
            link._client = new TestClientSocket(link._wire, profile, link._clock);

            link._client.connect(new InetSocketAddress("127.0.0.1",
                    link._server.getLocalPort()), CONNECT_TIMEOUT_MS);

            Assert.isTrue("connected", link._client.isConnected());

            link._peer.awaitConnection();

            /*
             * The handshake is done, so the client's timers go first and the
             * server's port is silenced after them: from here the only thing the
             * client hears is what this test sends, the only thing it can send
             * of its own accord is a null segment already on its way, and the
             * only thing that can make it retransmit is
             * runRetransmissionPass().
             */
            link._client.stopTimers();
            link._wire.passOnlyFrom(link._injector.getLocalPort());
            link.settleAckPoint();

            return link;
        }

        private Link()
            throws IOException
        {
            _clock = new ManualClock(1000000L);
            _wire = new Wire();
            _injector = new Injector();
            _peer = new Peer();
        }

        /**
         * Settles the number every acknowledgment in a case is stated against:
         * the last sequence number the client has spent on something that is
         * not data, which is behind every segment the application goes on to
         * write.
         * <p>
         * Behind it there is the handshake segment, and possibly the null
         * segment a connection sends behind that one - which is not something
         * to wait for. It is sent from a timer, and that timer only sends while
         * the send queue is empty, so a client whose handshake segment is still
         * queued when the timer first fires does not send one at all, and one
         * whose queue is empty does: which of the two happens is a race between
         * two threads. It matters here because the null segment occupies a slot
         * of the window while it is there and it is the head of the queue, so
         * where it lands decides both how many segments the writer may send and
         * which segment a report of a loss points at. So the timers are gone,
         * the wire is watched until it is quiet, and whatever control segments
         * did go out are acknowledged together - one cumulative acknowledgment
         * takes them all off the schedule.
         * <p>
         * Acknowledged twice, deliberately. Until the anchor has been
         * acknowledged the client treats whatever number arrives as the first
         * it has been told, so a case that wanted its duplicates to be
         * duplicates would depend on which of the peer's own acknowledgments
         * happened to be read last. Two leave the same state behind whichever
         * is dealt with first - the number is the anchor and no duplicate is
         * counted against it - and both are known to have been dealt with
         * rather than merely read off the socket, because the client reads one
         * datagram at a time.
         */
        private void settleAckPoint()
        {
            _injector.aimAt(_client.getLocalPort());

            _wire.awaitQuiet();

            int before = _wire.deliveredCount();

            _injector.ack(_wire.newestSetupSeq());
            _wire.awaitDelivered(before + 1);

            _anchor = _wire.handshakeSeq();

            _injector.ack(_anchor);
            _wire.awaitDelivered(before + 2);

            _injector.ack(_anchor);
            _wire.awaitDelivered(before + 3);

            _lastAcked = _wire.dataSeqs().length - 1;
        }

        /** A writer that keeps filling the window for the length of a case. */
        Writer writer()
        {
            if (_writer == null) {
                _writer = new Writer(_client);
                _writer.start();
            }

            return _writer;
        }

        /** The anchor every acknowledgment here repeats, long since acknowledged. */
        long anchorSeq()
        {
            return _anchor;
        }

        long firstUnackedSeq()
        {
            return _wire.dataSeqs()[_lastAcked + 1];
        }

        long lastUnackedSeq()
        {
            int[] seqs = _wire.dataSeqs();
            return seqs[seqs.length - 1];
        }

        /** The next one, for acknowledging a window's worth one segment at a time. */
        long nextUnackedSeq()
        {
            return _wire.dataSeqs()[++_lastAcked];
        }

        /**
         * Acknowledges everything outstanding, which empties the queue and so
         * lets the writer's next window be counted from nothing.
         */
        void acknowledgeEverything()
        {
            int[] seqs = _wire.dataSeqs();

            _lastAcked = seqs.length - 1;

            _injector.ack(seqs[_lastAcked]);
        }

        /**
         * Reads the window the client is holding the writer to: acknowledge
         * everything, so that the queue is empty, and count what the writer was
         * then allowed to send.
         */
        int readWindow()
        {
            writer().mark();
            acknowledgeEverything();

            return writer().fill();
        }

        Wire wire()
        {
            return _wire;
        }

        Injector injector()
        {
            return _injector;
        }

        void awaitDataSegments(int expected)
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

            while (System.currentTimeMillis() < deadline) {
                int sent = _wire.dataCount();

                if (sent == expected) {
                    return;
                }

                Assert.isTrue("sent " + sent + " data segments, expected " + expected +
                        " and never more", sent < expected);

                pause(STEP_MS);
            }

            throw new AssertionError("only " + _wire.dataCount() + " of " + expected +
                    " data segments went out within " + INJECTION_TIMEOUT_MS + "ms");
        }

        /** Steps the clock past the timeout every segment sent so far was given. */
        void advanceToTimeout()
        {
            _clock.advance((int) FAR_FUTURE_MS);
        }

        void runSchedule()
        {
            _client.runSchedule();
        }

        void close()
        {
            if (_writer != null) {
                _writer.halt();
            }

            closeQuietly(_client);
            _peer.shutdown();
            closeQuietly(_server);
            closeQuietly(_injector);
            closeQuietly(_wire);
        }

        private final ManualClock _clock;
        private final Wire _wire;
        private final Injector _injector;
        private final Peer _peer;

        private ReliableServerSocket _server;
        private TestClientSocket _client;
        private Writer _writer;
        private int _lastAcked = -1;
        private int _anchor = -1;
    }

    /**
     * Writes segments for as long as the socket allows and stops when it does
     * not, which is what makes the window readable at all: the socket blocks
     * while it holds as many unacknowledged segments as its window allows, so
     * the count of what reached the wire before it stopped is the window.
     * <p>
     * The block is expected rather than waited out, so the thread is left
     * parked until the next acknowledgment frees it - which is the point, since
     * what the writer is allowed to send after each one is what the cases here
     * are about. Everything it does not need to know about the socket's
     * internals is therefore visible from outside it.
     */
    private static final class Writer extends Thread
    {
        Writer(ReliableSocket client)
        {
            super("CongestionControlTest-Writer");

            _client = client;
            setDaemon(true);
        }

        /**
         * Notes where the wire is, so that the next fill() counts from here.
         * <p>
         * Precondition: the writer is parked, which every caller's previous
         * fill() has established. Counting from a wire the writer is still
         * filling would mix one round trip's worth of segments into the next.
         */
        int mark()
        {
            _mark = _written;
            return _written;
        }

        /**
         * Waits for the writer to have been given room and to have used all of
         * it, and reports how many segments that came to.
         */
        int fill()
        {
            awaitProgress();
            awaitParked();

            return _written - _mark;
        }

        /** How many segments have gone out in total. */
        int written()
        {
            return _written;
        }

        void halt()
        {
            _stopped = true;
        }

        public void run()
        {
            try {
                OutputStream out = _client.getOutputStream();

                while (!_stopped) {
                    out.write(SEGMENT_PAYLOAD);
                    out.flush();

                    _written++;
                }
            }
            catch (IOException xcp) {
                /* the socket was closed under it, which ends the test anyway */
            }
        }

        /**
         * Waits for the writer to be parked on a window with no room left in
         * it. Waiting for the state rather than for a grace period is what
         * makes this an assertion: a thread that had only been slow to be
         * scheduled would leave the wire just as quiet, and would have gone on
         * to send a moment later.
         */
        private void awaitParked()
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

            while (getState() != Thread.State.WAITING &&
                   getState() != Thread.State.TERMINATED) {
                if (System.currentTimeMillis() >= deadline) {
                    throw new AssertionError("the writer was still in progress after " +
                            INJECTION_TIMEOUT_MS + "ms and had neither parked nor finished");
                }

                pause(STEP_MS);
            }

            Assert.equals("the writer is parked on a window with no room in it",
                    Thread.State.WAITING, getState());
        }

        private void awaitProgress()
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

            while (_written <= _mark) {
                if (System.currentTimeMillis() >= deadline) {
                    throw new AssertionError("no segment went out within " +
                            INJECTION_TIMEOUT_MS + "ms of the acknowledgment");
                }

                pause(STEP_MS);
            }
        }

        private final ReliableSocket _client;
        private volatile int _written;
        private volatile boolean _stopped;
        private int _mark;
    }

    /** A clock that only moves when the test says so. */
    private static final class ManualClock implements Clock
    {
        ManualClock(long start)
        {
            _now = start;
        }

        public long currentTimeMillis()
        {
            return _now;
        }

        public void advance(int millis)
        {
            _now += millis;
        }

        private volatile long _now;
    }

    /**
     * The datagram socket the client writes to, which keeps a copy of
     * everything that went out and decides what may come back in.
     */
    private static final class Wire extends DatagramSocket
    {
        Wire()
            throws IOException
        {
            super((SocketAddress) null);
        }

        /**
         * Delivers only what arrives from the given port and swallows the
         * rest; zero takes the filter off, which is the state a connection
         * starts in and the one the handshake has to be run under.
         * <p>
         * By source rather than by segment type, because this test's
         * acknowledgments have to be the real thing: a socket that let only
         * extended ones through would swallow the very segments driving it,
         * and one that silenced them by type could not send a data segment
         * without its acknowledgment going with it.
         */
        void passOnlyFrom(int port)
        {
            _acceptedPort = port;
        }

        /**
         * The sequence number the connection was opened with, which is behind
         * every segment the client goes on to send and therefore the one
         * number an acknowledgment of it cannot take anything off the schedule.
         * <p>
         * The handshake segment rather than the newest segment of any kind: the
         * client numbers the acknowledgments and reports of holes it sends as
         * it goes, and those numbers are of no use as an anchor - a report of a
         * hole in the peer's stream is numbered from the peer's sequence space
         * and can sit anywhere at all.
         */
        int handshakeSeq()
        {
            synchronized (_sent) {
                for (byte[] packet : _sent) {
                    Segment s = parseQuietly(packet);

                    if (s != null && "SYN".equals(s.type())) {
                        return s.seq();
                    }
                }
            }

            return -1;
        }

        /**
         * The sequence number of the last segment the client spent on setting
         * the connection up: the handshake segment's, or the null segment's
         * when one went out behind it. Acknowledging it takes both off the
         * schedule.
         * <p>
         * Only those two are looked at. The acknowledgments and the reports of
         * holes the client sends in answer to the peer are numbered from the
         * peer's sequence space rather than its own, so one of them can carry
         * any number at all - a report of a hole in what the peer has received
         * is numbered next after the last thing the peer had in order.
         */
        int newestSetupSeq()
        {
            synchronized (_sent) {
                int seq = -1;

                for (byte[] packet : _sent) {
                    Segment s = parseQuietly(packet);

                    if (s != null && ("SYN".equals(s.type()) || "NUL".equals(s.type()))) {
                        seq = s.seq();
                    }
                }

                return seq;
            }
        }

        /**
         * Waits until the client has sent nothing for a grace period, so that
         * whatever went out on its own account has been seen before the wire
         * is taken as the state the case starts from.
         */
        void awaitQuiet()
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;
            int seen = sentCount();
            long quietSince = System.currentTimeMillis();

            while (System.currentTimeMillis() < deadline) {
                int size = sentCount();

                if (size != seen) {
                    seen = size;
                    quietSince = System.currentTimeMillis();
                }
                else if (System.currentTimeMillis() - quietSince >= QUIET_GRACE_MS) {
                    return;
                }

                pause(STEP_MS);
            }

            throw new AssertionError("the client was still sending after " +
                    INJECTION_TIMEOUT_MS + "ms");
        }

        private int sentCount()
        {
            synchronized (_sent) {
                return _sent.size();
            }
        }

        /** How many datagrams have been handed to the client so far. */
        int deliveredCount()
        {
            synchronized (_sent) {
                return _delivered;
            }
        }

        /**
         * Waits for the client to have been handed at least this many
         * datagrams.
         * <p>
         * The client reads one at a time and in order, so a datagram it has
         * been handed proves the one before it was dealt with, and waiting for
         * a second proves the first was dealt with rather than merely read off
         * the socket. That is what makes an injected acknowledgment countable
         * at all, since nothing about the client reports what it has done.
         */
        void awaitDelivered(int atLeast)
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

            while (deliveredCount() < atLeast) {
                if (System.currentTimeMillis() >= deadline) {
                    throw new AssertionError("only " + deliveredCount() + " of " + atLeast +
                            " datagrams reached the client within " + INJECTION_TIMEOUT_MS + "ms");
                }

                pause(STEP_MS);
            }
        }

        int dataCount()
        {
            synchronized (_sent) {
                int count = 0;

                for (byte[] packet : _sent) {
                    if (isData(packet)) {
                        count++;
                    }
                }

                return count;
            }
        }

        /**
         * The sequence number of every data segment, in the order the client
         * first sent each. A retransmission adds nothing here: it is a segment
         * the client has already listed, and counting it twice would make
         * "the highest sequence number sent" a segment it stopped believing in
         * long ago.
         */
        int[] dataSeqs()
        {
            synchronized (_sent) {
                List<Integer> seqs = new ArrayList<Integer>();

                for (byte[] packet : _sent) {
                    Segment s = parseQuietly(packet);

                    if (s != null && "DAT".equals(s.type()) && !seqs.contains(s.seq())) {
                        seqs.add(s.seq());
                    }
                }

                int[] out = new int[seqs.size()];

                for (int i = 0; i < out.length; i++) {
                    out[i] = seqs.get(i);
                }

                return out;
            }
        }

        @Override
        public void send(DatagramPacket p)
            throws IOException
        {
            byte[] copy = new byte[p.getLength()];
            System.arraycopy(p.getData(), p.getOffset(), copy, 0, copy.length);

            synchronized (_sent) {
                _sent.add(copy);
            }

            super.send(p);
        }

        @Override
        public void receive(DatagramPacket p)
            throws IOException
        {
            while (true) {
                super.receive(p);

                synchronized (_sent) {
                    if (_acceptedPort != 0 && p.getPort() != _acceptedPort) {
                        continue;
                    }

                    _delivered++;
                }

                return;
            }
        }

        private static Segment parseQuietly(byte[] packet)
        {
            try {
                return Segment.parse(packet);
            }
            catch (RuntimeException xcp) {
                return null;
            }
        }

        private static boolean isData(byte[] packet)
        {
            Segment s = parseQuietly(packet);
            return s != null && "DAT".equals(s.type());
        }

        private int _acceptedPort;
        private int _delivered;
        private final List<byte[]> _sent = new ArrayList<byte[]>();
    }

    /**
     * Sends crafted segments to the client, standing in for the peer: plain
     * acknowledgments that take segments off the schedule, extended ones that
     * report the holes it has, and data segments carrying an acknowledgment of
     * their own the way a peer with traffic of its own sends them.
     * <p>
     * A socket of its own rather than the peer's, because the test has to
     * decide exactly what arrives and when, and a peer under a client whose
     * acknowledgments are being swallowed has no way to send any of it. The
     * segments are the real ones on the wire - the socket's own classes
     * serialize themselves - so the client's parsing and handling are
     * exercised rather than stubbed.
     */
    private static final class Injector extends DatagramSocket
    {
        Injector()
            throws IOException
        {
            super(new InetSocketAddress("127.0.0.1", 0));
        }

        void aimAt(int port)
        {
            _target = new InetSocketAddress("127.0.0.1", port);
        }

        /** Acknowledges everything up to and including the given sequence number. */
        void ack(long seqn)
        {
            sendTo(new ACKSegment(0, (int) seqn).getBytes());
        }

        /**
         * Reports a hole between the acknowledgment and what the peer does
         * have after it.
         * <p>
         * With both numbers the same the hole is empty, so nothing is
         * retransmitted and the client's EAK recovery stays out of a case that
         * is only about what the acknowledgment did to the window.
         */
        void eak(long lastInSequence, long outOfSequence)
        {
            sendTo(new EAKSegment(0, (int) lastInSequence,
                    new int[] { (int) outOfSequence }).getBytes());
        }

        /** A data segment from the peer, carrying an acknowledgment of its own. */
        void dat(int seqn, long ackn)
        {
            sendTo(new DATSegment(seqn, (int) ackn, SEGMENT_PAYLOAD, 0,
                    SEGMENT_PAYLOAD.length).getBytes());
        }

        private void sendTo(byte[] packet)
        {
            if (_target == null) {
                throw new AssertionError("the injector was never aimed at a port");
            }

            try {
                send(new DatagramPacket(packet, packet.length, _target));
            }
            catch (IOException xcp) {
                throw new AssertionError("could not inject a segment: " + xcp);
            }
        }

        private volatile InetSocketAddress _target;
    }

    /** The server end: accepts one connection and drains whatever arrives. */
    private static final class Peer
    {
        ReliableServerSocket start()
            throws IOException
        {
            _server = new ReliableServerSocket(0, 16);
            _thread = new Thread(this::run, "CongestionControlTest-Peer");
            _thread.setDaemon(true);
            _thread.start();

            return _server;
        }

        private void run()
        {
            Socket accepted = null;

            try {
                accepted = _server.accept();
                _accepted = accepted;
                _connected.countDown();

                byte[] buffer = new byte[4096];

                while (accepted.getInputStream().read(buffer) > 0) {
                    /* keep draining, so the peer's receive queue never fills */
                }
            }
            catch (IOException xcp) {
                /* the socket was closed under us, which ends the peer */
            }
            finally {
                if (accepted != null) {
                    closeQuietly(accepted);
                }
            }
        }

        void awaitConnection()
            throws InterruptedException
        {
            if (!_connected.await(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new AssertionError("the server never accepted the connection");
            }
        }

        void shutdown()
        {
            if (_server != null) {
                closeQuietly(_server);
            }

            /*
             * The server socket's close() deliberately leaves a live client
             * connection alone, so the read above stays parked and the join
             * would always run out its timeout - two dead seconds in a suite
             * that touches no real clock. Closing the accepted socket is what
             * unblocks it.
             */
            closeQuietly(_accepted);

            if (_thread != null) {
                try {
                    _thread.join(2000);
                }
                catch (InterruptedException xcp) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private ReliableServerSocket _server;
        private Thread _thread;
        private volatile Socket _accepted;
        private final CountDownLatch _connected = new CountDownLatch(1);
    }

    /**
     * A client whose retransmission schedule the test can run by hand. Both
     * seams are protected on the socket, so they are re-exposed here: only a
     * subclass may reach them.
     */
    private static final class TestClientSocket extends ReliableSocket
    {
        TestClientSocket(DatagramSocket sock, ReliableSocketProfile profile, Clock clock)
        {
            super(sock, profile, clock);
        }

        /** Stops the timer threads so the test is the only thing that retries. */
        void stopTimers()
        {
            destroyTimers();
        }

        /** Runs one pass of the schedule at the current instant. */
        void runSchedule()
        {
            runRetransmissionPass();
        }
    }
}
