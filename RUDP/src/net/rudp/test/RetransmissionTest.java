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

package net.rudp.test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.rudp.Clock;
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketProfile;
import net.rudp.impl.ACKSegment;
import net.rudp.impl.EAKSegment;
import net.rudp.impl.Segment;

/**
 * Tests the retransmission schedule of a live connection against a clock the
 * test advances by hand.
 * <p>
 * The client is connected to a real server over loopback so the handshake and
 * the queues are genuine, then the acknowledgments are cut off and the timer
 * threads are torn down, leaving the test as the only thing that can trigger a
 * retransmission. That makes every instant in this file an exact number rather
 * than "eventually, if the machine was not busy": the gap between two
 * retransmissions is the thing under test, and a gap measured with a real
 * clock cannot distinguish an exponential backoff from a constant one, or from
 * no backoff at all.
 */
public class RetransmissionTest
{
    /**
     * Long enough for the handshake, short enough to bound a broken run.
     * <p>
     * Note that connect() decides whether it timed out by comparing this value
     * against the socket's own clock, which here is frozen: the wait itself is
     * real time, but the comparison never becomes true. A handshake that stalls
     * therefore surfaces as a connection-refused, never as a
     * SocketTimeoutException - which is fine here, since both are failures and
     * the test asserts nothing about that exception. Passing 0 instead would
     * park the test thread forever.
     */
    private static final int CONNECT_TIMEOUT_MS = 10000;

    /** One step of the clock walk; small enough to locate a deadline exactly. */
    private static final int STEP_MS = 1;

    /** How far the walk is willing to search for the next transmission. */
    private static final int SEARCH_LIMIT_MS = 60000;

    /**
     * How long to wait for the reader thread to have dealt with an injected
     * segment. Real time, but only for thread scheduling: the reader picks
     * datagrams off the socket by itself, so a retransmission it decides to
     * make becomes observable a moment later rather than instantly. No
     * protocol timing is measured with this clock - that is what the manual
     * clock is for.
     */
    private static final int INJECTION_TIMEOUT_MS = 5000;

    /**
     * How long a wire is watched to conclude that nothing more is coming.
     */
    private static final int QUIET_GRACE_MS = 250;

    /**
     * Enough manual time to outlast any deadline a segment can be given: the
     * backoff is capped at MAX_BACKOFF_SHIFT doublings of a timeout that is
     * itself capped, so nothing stays due beyond this.
     */
    private static final long FAR_FUTURE_MS = 10L * 60L * 1000L;

    public static void main(String[] args)
    {
        Assert.suite("retransmission schedule");

        testBackoffSchedule();
        testEakDrivenRecovery();

        System.exit(Assert.report());
    }

    private static void testBackoffSchedule()
    {
        Assert.test("retries a lost segment on a doubling timeout, then gives up", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Peer peer = new Peer();
            Injector injector = new Injector();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(2), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                Assert.isTrue("connected", client.isConnected());

                peer.awaitConnection();
                settleNullSegment(client, wire, injector);

                /*
                 * From here on the test owns the schedule: with the timers gone
                 * the only path to a retransmission is runRetransmissionPass(),
                 * so nothing can fire on its own and race the walk.
                 */
                client.stopTimers();

                wire.cutOff();

                client.getOutputStream().write(new byte[] { 1, 2, 3, 4, 5 });
                client.getOutputStream().flush();

                long sentAt = clock.currentTimeMillis();
                Assert.equals("the payload went out as one data segment", 1, wire.dataCount());

                long firstRetry = walkToNextDataSegment(client, wire, clock, sentAt, 2);
                long timeout = firstRetry - sentAt;

                Assert.isTrue("the first retry waited for a timeout (" + timeout + "ms)",
                        timeout > 0);

                long secondRetry = walkToNextDataSegment(client, wire, clock, firstRetry, 3);

                Assert.equals("the second retry doubled the timeout",
                        firstRetry + 2 * timeout, secondRetry);

                /*
                 * Keep going until the retransmission limit runs out. Which of
                 * the pending segments gives up first depends on the order the
                 * schedule happens to hold them in, so all this asserts is that
                 * the connection is torn down rather than retried forever.
                 */
                long gaveUpAt = walkUntilClosed(client, clock, secondRetry);

                Assert.isTrue("the connection was given up on", gaveUpAt > 0);
                Assert.isFalse("still connected", client.isConnected());

                /*
                 * The profile allows two retransmissions and the payload went
                 * out once, so three data segments is all that may ever reach
                 * the wire. The timeout that ran out of attempts has to give up
                 * instead of sending one more, and nothing else may have been
                 * sent either: the walk stops on the first segment that reaches
                 * its target, so an extra packet on the way there would have
                 * gone unnoticed.
                 */
                Assert.equals("the payload and its two retransmissions, and no more",
                        3, wire.dataCount());
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
                injector.close();
                wire.close();
            }
        });

        Assert.test("gives each segment its own deadline", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Peer peer = new Peer();
            Injector injector = new Injector();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(2), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                peer.awaitConnection();
                settleNullSegment(client, wire, injector);
                client.stopTimers();
                wire.cutOff();

                long firstSentAt = clock.currentTimeMillis();
                write(client, new byte[] { 1, 2, 3, 4, 5 });

                /*
                 * Send the second segment a fixed gap later. Both carry the
                 * same timeout, so the second one's deadline has to sit exactly
                 * that much later than the first one's - the schedule is
                 * per segment, not a single tick that sweeps the whole window.
                 */
                int gap = 30;
                clock.advance(gap);
                write(client, new byte[] { 6, 7, 8, 9, 10 });

                Assert.equals("both segments went out", 2, wire.dataCount());

                long firstRetry = walkToNextDataSegment(client, wire, clock, firstSentAt, 3);
                Assert.equals("only the segment that timed out was retried", 3, wire.dataCount());

                long secondRetry = walkToNextDataSegment(client, wire, clock, firstRetry, 4);
                Assert.equals("the second deadline is the first one shifted by the send gap",
                        firstRetry + gap, secondRetry);
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
                injector.close();
                wire.close();
            }
        });

        Assert.test("a pass before any deadline retransmits nothing", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Peer peer = new Peer();
            Injector injector = new Injector();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(2), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                peer.awaitConnection();
                settleNullSegment(client, wire, injector);
                client.stopTimers();
                wire.cutOff();

                for (int i = 0; i < 3; i++) {
                    write(client, new byte[] { 1, 2, 3, 4, 5 });
                }

                Assert.equals("three segments went out", 3, wire.dataCount());

                /*
                 * A retransmission timer used to resend every unacknowledged
                 * segment on every tick, which turns a single loss into a storm
                 * on an already congested path. Standing still in time and
                 * hammering the schedule must stay completely quiet.
                 */
                for (int i = 0; i < 1000; i++) {
                    client.runSchedule();
                }

                Assert.equals("nothing was retransmitted", 3, wire.dataCount());
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
                injector.close();
                wire.close();
            }
        });
    }

    private static void write(ReliableSocket client, byte[] payload)
        throws IOException
    {
        client.getOutputStream().write(payload);
        client.getOutputStream().flush();
    }

    /**
     * Settles the null segment the socket sends the moment a connection is
     * opened, so that everything after this point has to reason only about the
     * segments the test writes itself.
     * <p>
     * Two things about that segment are none of the test's business, and both
     * are settled here by acknowledging it rather than by changing the socket:
     * <ul>
     * <li>It carries a sequence number, so it has to be on the wire before the
     *     first segment the test writes. Otherwise it lands between two of
     *     them, and an extended acknowledgment naming the hole between those
     *     two finds the null segment at its head and fills the wrong
     *     hole.</li>
     * <li>It is acknowledged like anything else, and every acknowledgment is
     *     one round trip sample. Against this frozen clock every sample is
     *     zero, so how many of them the estimator saw decides how far its
     *     estimate has converged - and with it every deadline the checks
     *     below are stated in. Acknowledging it here pins the count at exactly
     *     one: whichever acknowledgment reaches the client first takes it off
     *     the schedule and supplies the sample, and the one after it finds an
     *     empty queue and takes nothing. Whichever order they arrive in, the
     *     test can no longer tell.</li>
     * </ul>
     * Two acknowledgments go out so that the first is known to have been dealt
     * with rather than merely picked up: the client reads one datagram at a
     * time, so the second can only be read after the first has been handled.
     */
    private static void settleNullSegment(TestClientSocket client, Wire wire, Injector injector)
    {
        wire.awaitNullSegment();
        injector.aimAt(client.getLocalPort());

        int before = wire.receivedCount();
        int nul = wire.nullSegmentSeq();

        injector.sendAck(nul);
        wire.awaitReceived(before + 1);

        injector.sendAck(nul);
        wire.awaitReceived(before + 2);
    }


    /**
     * Advances the clock one step at a time, running a retransmission pass at
     * every instant, until the given number of data segments has been sent.
     * A pass taken before a deadline is due changes nothing, so the walk can
     * probe freely and the instant it stops on is the deadline itself.
     * <p>
     * More segments than asked for is a failure and not something to walk past:
     * the walk stops as soon as the count is reached, so an extra packet sent
     * early would otherwise shorten the next interval and be reported as a
     * correct deadline. Caught here, it is reported as what it is.
     *
     * @return the instant at which the segment became due.
     */
    private static long walkToNextDataSegment(TestClientSocket client, Wire wire,
            ManualClock clock, long from, int targetCount)
    {
        for (int step = 0; step <= SEARCH_LIMIT_MS; step += STEP_MS) {
            clock.advance(STEP_MS);
            client.runSchedule();

            int sent = wire.dataCount();

            if (sent > targetCount) {
                throw new AssertionError("sent " + sent + " data segments at " +
                        clock.currentTimeMillis() + ", before the " + targetCount +
                        "th was due after " + from);
            }

            if (sent >= targetCount) {
                return clock.currentTimeMillis();
            }
        }

        throw new AssertionError("no retransmission within " + SEARCH_LIMIT_MS +
                "ms after " + from + " (still at " + wire.dataCount() + " data segments)");
    }

    private static long walkUntilClosed(TestClientSocket client, ManualClock clock, long from)
    {
        for (int step = 0; step <= SEARCH_LIMIT_MS; step += STEP_MS) {
            if (!client.isConnected()) {
                return clock.currentTimeMillis();
            }

            clock.advance(STEP_MS);
            client.runSchedule();
        }

        return client.isConnected() ? -1 : clock.currentTimeMillis();
    }

    /**
     * Recovery driven by the peer's explicit "I am missing these" reports,
     * rather than by the local schedule.
     */
    private static void testEakDrivenRecovery()
    {
        Assert.test("an EAK fills its hole once, with the retry backed off", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Injector injector = new Injector();
            Peer peer = new Peer();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(4), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                peer.awaitConnection();
                settleNullSegment(client, wire, injector);
                client.stopTimers();

                /*
                 * Nothing acknowledges the payload: an ack would take the
                 * segments off the schedule and close the hole on its own,
                 * which is not what is under test here. Only EAKs are let
                 * through, and they come from an injector rather than from the
                 * peer so that the test decides how many arrive and when -
                 * a peer whose acks are being swallowed never gets the chance
                 * to send any.
                 */
                wire.passExtendedAcksOnly();

                for (int i = 0; i < 5; i++) {
                    write(client, new byte[] { 1, 2, 3, 4, 5 });
                }

                int[] seqs = wire.dataSeqs();
                Assert.equals("five segments went out", 5, seqs.length);

                injector.aimAt(client.getLocalPort());
                long at = clock.currentTimeMillis();

                /*
                 * The receiver holds the first segment and the last two and is
                 * missing the two in between: the exact shape an EAK describes,
                 * where the hole is what lies between its ack number and its
                 * last out-of-sequence ack number.
                 */
                injector.sendEak(seqs[0], new int[] { seqs[3], seqs[4] });

                awaitDataSegments(wire, 6);

                Assert.arrayEquals("the head of the hole",
                        new int[] { seqs[1] }, wire.lastDataSeqs(1));

                /*
                 * The next EAK says exactly the same thing, which is what most
                 * of them do: one goes out per few out-of-order arrivals, so a
                 * window's worth of loss produces a stream of them. Refilling
                 * the hole for each is what turned a single loss into a
                 * multiple of the window.
                 */
                injector.sendEak(seqs[0], new int[] { seqs[3], seqs[4] });
                assertQuiet(wire, 6);

                /*
                 * Two segments of the five are still out: the head of the
                 * hole, which the EAK retransmitted, and the one behind it,
                 * which it did not.  Neither was named by the backoff, so the
                 * second gives the walk its reference - its deadline is the
                 * bare RTO, while the hole's is that same RTO doubled, since
                 * the retransmission the EAK caused counts as a timeout as far
                 * as the schedule is concerned (RFC 6298 5.5).
                 */
                long firstRetry = walkToNextDataSegment(client, wire, clock, at, 7);
                long timeout = firstRetry - at;

                Assert.equals("the segment the EAK never touched timed out first",
                        1, wire.dataCount() - 6);

                long secondRetry = walkToNextDataSegment(client, wire, clock, firstRetry, 8);

                /*
                 * Anchored at the send, not at the reference's retry: the hole's
                 * deadline was stamped when the EAK caused its retransmission,
                 * which the frozen clock puts at the same instant as the
                 * original send. Doubling from there is what makes the next
                 * attempt wait 2*RTO rather than the bare RTO it would have
                 * waited without the backoff.
                 */
                Assert.equals("the hole's retries doubled its timeout",
                        at + 2 * timeout, secondRetry);
                Assert.equals("the hole head went out again",
                        1, wire.dataCount() - 7);

                /*
                 * The EAK cut the window in half when it came in, and nothing
                 * has been acknowledged since, so the flight is the same two
                 * segments and the window has no room left in it.  Nothing may
                 * go out until it does - a window that lets one segment past
                 * anyway is not holding the sender back at all, which is how
                 * the connection ended up writing at full speed over a path it
                 * had just been told was congested.
                 */
                int quietAt = assertStalledWrite(client, wire);

                /*
                 * And an acknowledgment is what releases it: the window counts
                 * segments in flight, so one arriving frees one slot and the
                 * write that was waiting goes through - which is also what
                 * tells the two cases apart. A sender held back until an
                 * acknowledgment arrives is congestion control; one that stops
                 * for any other reason is a stall.
                 */
                wire.passPlainAcksToo();
                injector.sendAck(seqs[4]);
                awaitDataSegments(wire, quietAt + 1);
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
                injector.close();
                wire.close();
            }
        });

        Assert.test("an EAK is honoured again once the hole is due", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Injector injector = new Injector();
            Peer peer = new Peer();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(4), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                peer.awaitConnection();
                settleNullSegment(client, wire, injector);
                client.stopTimers();
                wire.passExtendedAcksOnly();

                for (int i = 0; i < 5; i++) {
                    write(client, new byte[] { 1, 2, 3, 4, 5 });
                }

                int[] seqs = wire.dataSeqs();
                Assert.equals("five segments went out", 5, seqs.length);

                injector.aimAt(client.getLocalPort());
                injector.sendEak(seqs[0], new int[] { seqs[3], seqs[4] });
                awaitDataSegments(wire, 6);

                /*
                 * The timers are torn down in this harness, so the hole's own
                 * deadline can pass without anything acting on it - which is
                 * the situation the EAK path exists for. Step past every
                 * deadline a segment could have been given and report the hole
                 * again: a gate that had been closed to a segment after its
                 * first retransmission would swallow this and leave the
                 * connection stuck for good.
                 */
                clock.advance((int) FAR_FUTURE_MS);
                injector.sendEak(seqs[0], new int[] { seqs[3], seqs[4] });

                awaitDataSegments(wire, 7);

                Assert.arrayEquals("the hole head was filled a second time",
                        new int[] { seqs[1] }, wire.lastDataSeqs(1));
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
                injector.close();
                wire.close();
            }
        });
    }

    /**
     * Waits for the reader thread to have acted on an injected segment.
     * <p>
     * Overshooting the expected count is a failure and not something to wait
     * out: an extra segment is the whole point of the checks that follow, so
     * it is reported the moment it appears rather than after the wait.
     */
    private static void awaitDataSegments(Wire wire, int expected)
    {
        long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            int sent = wire.dataCount();

            if (sent == expected) {
                return;
            }

            Assert.isTrue("sent " + sent + " data segments, expected " + expected +
                    " and never more", sent < expected);

            sleep(STEP_MS);
        }

        throw new AssertionError("only " + wire.dataCount() + " of " + expected +
                " data segments went out within " + INJECTION_TIMEOUT_MS + "ms");
    }

    /**
     * Requires nothing new to reach the wire within a grace period.
     */
    private static void assertQuiet(Wire wire, int expected)
    {
        sleep(QUIET_GRACE_MS);
        Assert.equals("nothing else went out", expected, wire.dataCount());
    }

    /**
     * Requires that a write does not get onto the wire, and returns the count
     * the wire was left at.
     * <p>
     * The write is made on a thread of its own and is expected to stay where
     * it is: the sender is not allowed to put anything more on a path than
     * the window it has been given, and the window here has no room left in
     * it. Waiting for the thread to park rather than merely to start is what
     * makes this an assertion rather than a grace period - a thread that had
     * only been slow to be scheduled would leave the wire just as quiet, and
     * would have gone on to send the segment a moment later.
     * <p>
     * The thread is left parked. It is a daemon and the socket's close wakes
     * whatever is waiting on its window, so it cannot outlive the suite.
     */
    private static int assertStalledWrite(ReliableSocket client, Wire wire)
    {
        int before = wire.dataCount();

        Thread writer = new Thread(() -> {
            try {
                client.getOutputStream().write(new byte[] { 6, 7, 8, 9, 10 });
                client.getOutputStream().flush();
            }
            catch (IOException xcp) {
                /* the socket was closed under it, which ends the test anyway */
            }
        }, "RetransmissionTest-StalledWrite");

        writer.setDaemon(true);
        writer.start();

        long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

        while (writer.getState() != Thread.State.WAITING &&
               writer.getState() != Thread.State.TERMINATED) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError("the write was still in progress after " +
                        INJECTION_TIMEOUT_MS + "ms and had neither parked nor finished");
            }

            sleep(STEP_MS);
        }

        Assert.equals("the write is parked on a window with no room in it",
                Thread.State.WAITING, writer.getState());

        sleep(QUIET_GRACE_MS);

        Assert.equals("nothing went out while the window was full",
                before, wire.dataCount());

        return before;
    }

    private static void sleep(int millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException xcp) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting");
        }
    }

    private static ReliableSocketProfile profile(int maxRetrans)
    {
        /*
         * A finite retransmission limit is what makes the give-up path
         * reachable at all; the default profile retries forever. The EAK cases
         * ask for a few more attempts than that path needs, so that an extra
         * retransmission shows up as an extra segment on the wire instead of
         * quietly ending the connection.
         */
        return new ReliableSocketProfile(96, 96, 1200, 64, maxRetrans, 3, 3, 3, 2000, 200, 300);
    }

    private static void closeQuietly(Socket socket)
    {
        try {
            socket.close();
        }
        catch (IOException xcp) {
            /* the test is over either way */
        }
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

    /** A datagram socket that keeps a copy of everything sent and can go quiet. */
    private static final class Wire extends DatagramSocket
    {
        Wire()
            throws IOException
        {
            super((SocketAddress) null);
        }

        void cutOff()
        {
            _cutOff = true;
        }

        /**
         * Lets extended acknowledgments through and swallows everything else.
         * <p>
         * An EAK reports a hole and is retransmitted by nobody, so it is the
         * only segment a test can inject to drive recovery from the peer's
         * side; the acks that would resolve the hole on their own are exactly
         * what has to be dropped.
         */
        void passExtendedAcksOnly()
        {
            _onlyExtendedAcks = true;
        }

        /**
         * Lets plain acknowledgments through as well, so that an injected one
         * can be read by the client.  Anything the peer had outstanding has
         * been acknowledged already by the time this is called, so the peer
         * has nothing left to add to the wire.
         */
        void passPlainAcksToo()
        {
            _onlyExtendedAcks = false;
        }

        /**
         * Waits for the null segment a connection sends the moment it is
         * opened.
         * <p>
         * The socket schedules it with no delay (see
         * ReliableSocket.connectionOpened), on a timer thread of its own and
         * in real time, so it is always sent - and it is sent concurrently
         * with whatever the test does next. Since it carries a sequence number
         * of its own, a segment written while it is still in flight lands
         * between it and the segment after, and an extended acknowledgment
         * naming a hole then walks into the null segment first and fills the
         * wrong hole.
         */
        void awaitNullSegment()
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

            while (System.currentTimeMillis() < deadline) {
                if (nullSegmentSeq() >= 0) {
                    return;
                }

                sleep(STEP_MS);
            }

            throw new AssertionError("no null segment went out within " +
                    INJECTION_TIMEOUT_MS + "ms of the connection being opened");
        }

        /**
         * The sequence number the null segment went out with, or -1 if none has
         * gone out yet.
         */
        int nullSegmentSeq()
        {
            synchronized (_sent) {
                for (byte[] packet : _sent) {
                    Segment s = parseQuietly(packet);

                    if (s != null && "NUL".equals(s.type())) {
                        return s.seq();
                    }
                }
            }

            return -1;
        }

        /**
         * How many datagrams the client has picked off the socket so far.
         */
        int receivedCount()
        {
            synchronized (_sent) {
                return _received;
            }
        }

        /**
         * Waits for the client to have picked up at least the given number of
         * datagrams.
         * <p>
         * This is what makes an ordering deterministic where a clock cannot:
         * the client reads one datagram at a time and in order, so a datagram
         * that has been picked up proves the one before it was dealt with, and
         * waiting for a second one proves the first was dealt with rather than
         * merely read off the socket.
         */
        void awaitReceived(int atLeast)
        {
            long deadline = System.currentTimeMillis() + INJECTION_TIMEOUT_MS;

            while (System.currentTimeMillis() < deadline) {
                if (receivedCount() >= atLeast) {
                    return;
                }

                sleep(STEP_MS);
            }

            throw new AssertionError("only " + receivedCount() + " of " + atLeast +
                    " datagrams reached the client within " + INJECTION_TIMEOUT_MS + "ms");
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
         * Sequence numbers of every data segment sent, in order.
         */
        int[] dataSeqs()
        {
            synchronized (_sent) {
                int[] seqs = new int[dataCount()];
                int i = 0;

                for (byte[] packet : _sent) {
                    Segment s = parseQuietly(packet);

                    if (s != null && "DAT".equals(s.type())) {
                        seqs[i++] = s.seq();
                    }
                }

                return seqs;
            }
        }

        /**
         * Sequence numbers of the last few data segments sent, in order.
         */
        int[] lastDataSeqs(int n)
        {
            int[] all = dataSeqs();
            return Arrays.copyOfRange(all, Math.max(0, all.length - n), all.length);
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
                    _received++;
                }

                if (_cutOff) {
                    /*
                     * Swallow the datagram and keep waiting, so the socket's
                     * reader thread stays parked on the socket instead of
                     * processing an acknowledgment and taking the segment off
                     * the schedule.
                     */
                    continue;
                }

                if (_onlyExtendedAcks && !isExtendedAck(p)) {
                    continue;
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

        private static boolean isExtendedAck(DatagramPacket p)
        {
            try {
                return "EAK".equals(Segment.parse(p.getData(), p.getOffset(), p.getLength()).type());
            }
            catch (RuntimeException xcp) {
                return false;
            }
        }

        private volatile boolean _cutOff;
        private volatile boolean _onlyExtendedAcks;
        private int _received;
        private final List<byte[]> _sent = new ArrayList<byte[]>();
    }

    /**
     * Sends crafted acknowledgment segments to the client, standing in for the
     * peer: extended acknowledgments that report the holes it has, and plain
     * ones that take a segment off the schedule.
     * <p>
     * A plain socket on its own port rather than the peer's: the test needs to
     * decide exactly how many acknowledgments arrive and when, and the peer
     * under a client whose acks are being swallowed has no way to send any. The
     * segments are the real thing on the wire - EAKSegment and ACKSegment
     * serialize themselves - so the client's parsing and handling are
     * exercised, not stubbed.
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

        void sendEak(int lastInSequence, int[] outOfSequence)
        {
            sendTo(new EAKSegment(0, lastInSequence, outOfSequence).getBytes());
        }

        /**
         * Acknowledges everything up to and including the given sequence
         * number, the way the peer would.
         */
        void sendAck(int lastInSequence)
        {
            sendTo(new ACKSegment(0, lastInSequence).getBytes());
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
            _thread = new Thread(this::run, "RetransmissionTest-Peer");
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

                byte[] buffer = new byte[1024];

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
                _server.close();
            }

            /*
             * The server socket's close() deliberately leaves a live client
             * connection alone, so the read() below stays parked on the
             * accepted socket and the join would always run out its timeout -
             * two dead seconds in a suite that is supposed to touch no real
             * clock at all. Closing the accepted socket is what unblocks it.
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
     * A client whose retransmission schedule the test can run by hand. The two
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
