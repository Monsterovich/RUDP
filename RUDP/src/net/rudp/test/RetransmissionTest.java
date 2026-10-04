package net.rudp.test;

import java.io.IOException;
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

    public static void main(String[] args)
    {
        Assert.suite("retransmission schedule");

        testBackoffSchedule();

        System.exit(Assert.report());
    }

    private static void testBackoffSchedule()
    {
        Assert.test("retries a lost segment on a doubling timeout, then gives up", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Peer peer = new Peer();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                Assert.isTrue("connected", client.isConnected());

                /*
                 * From here on the test owns the schedule: with the timers gone
                 * the only path to a retransmission is runRetransmissionPass(),
                 * so nothing can fire on its own and race the walk.
                 */
                client.stopTimers();
                peer.awaitConnection();

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
                wire.close();
            }
        });

        Assert.test("gives each segment its own deadline", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Peer peer = new Peer();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                client.stopTimers();
                peer.awaitConnection();
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
                wire.close();
            }
        });

        Assert.test("a pass before any deadline retransmits nothing", () -> {
            ManualClock clock = new ManualClock(1000000L);
            Wire wire = new Wire();
            Peer peer = new Peer();

            ReliableServerSocket server = peer.start();
            TestClientSocket client = new TestClientSocket(wire, profile(), clock);

            try {
                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                client.stopTimers();
                peer.awaitConnection();
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

    private static ReliableSocketProfile profile()
    {
        /*
         * A finite retransmission limit is what makes the give-up path
         * reachable at all; the default profile retries forever.
         */
        return new ReliableSocketProfile(96, 96, 1200, 64, 2, 3, 3, 3, 2000, 200, 300);
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

        int dataCount()
        {
            int count = 0;

            synchronized (_sent) {
                for (byte[] packet : _sent) {
                    if (isData(packet)) {
                        count++;
                    }
                }
            }

            return count;
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
            while (_cutOff) {
                /*
                 * Swallow the datagram and keep waiting, so the socket's reader
                 * thread stays parked on the socket instead of processing an
                 * acknowledgment and taking the segment off the schedule.
                 */
                super.receive(p);
            }

            super.receive(p);
        }

        private static boolean isData(byte[] packet)
        {
            try {
                return "DAT".equals(Segment.parse(packet).type());
            }
            catch (RuntimeException xcp) {
                return false;
            }
        }

        private volatile boolean _cutOff;
        private final List<byte[]> _sent = new ArrayList<byte[]>();
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
