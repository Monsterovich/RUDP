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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketListener;
import net.rudp.ReliableSocketProfile;
import net.rudp.ReliableSocketStateListener;

/**
 * Exercises the connection state machine end to end: opening, the events a
 * listener sees, a graceful close that is visible to the peer as EOF, and a
 * reset that arrives from the network rather than from a local call.
 * <p>
 * The client uses a fast profile so that a close does not spend the default
 * several seconds waiting the duplicate-close timer out. The server adopts the
 * client's proposed timeouts during the handshake, so both ends move quickly.
 */
public class ConnectionLifecycleTest
{
    private static final int AWAIT_MS = 10000;
    private static final int FAST = 100;

    public static void main(String[] args)
    {
        Assert.suite("connection lifecycle");

        testConnectionOpenedFires();
        testGracefulCloseDeliversEof();
        testConnectionClosedFires();
        testResetFromPeerFiresConnectionReset();
        testDataListenerFiresOnce();

        System.exit(Assert.report());
    }

    private static void testConnectionOpenedFires()
    {
        Assert.test("connect reports the connection opened", () -> {
            Pair pair = new Pair();

            try {
                pair.connect();

                Assert.isTrue("client sees the open event",
                        pair.clientState.opened.await(2, TimeUnit.SECONDS));
                Assert.equals("opened exactly once", 1, pair.clientState.openedCount.get());
                Assert.isTrue("client is connected", pair.client.isConnected());
                Assert.isTrue("server side is connected", pair.accepted.isConnected());
            }
            finally {
                pair.close();
            }
        });
    }

    private static void testGracefulCloseDeliversEof()
    {
        Assert.test("a close is seen by the peer as end of stream", () -> {
            Pair pair = new Pair();

            try {
                pair.connect();

                pair.client.getOutputStream().write(new byte[] { 42, 43, 44 });
                pair.client.getOutputStream().flush();

                byte[] buffer = new byte[3];
                readFully(pair.accepted.getInputStream(), buffer);
                Assert.arrayEquals("bytes arrived", new byte[] { 42, 43, 44 }, buffer);

                pair.client.close();

                /*
                 * A FIN pushed the connection to CLOSE_WAIT; the next read must
                 * report EOF rather than block or throw. The read is bounded by
                 * the server's timeout so a regression here fails the test
                 * instead of hanging the suite.
                 */
                pair.accepted.setSoTimeout(5000);
                Assert.equals("peer reads EOF", -1, pair.accepted.getInputStream().read());
            }
            finally {
                pair.close();
            }
        });
    }

    private static void testConnectionClosedFires()
    {
        Assert.test("close reports the connection closed to listeners", () -> {
            Pair pair = new Pair();

            try {
                pair.connect();
                pair.client.close();

                /*
                 * close() returns before the duplicate-close timer expires, so
                 * the event lands a moment later on a helper thread.
                 */
                Assert.isTrue("closed event arrived",
                        pair.clientState.closed.await(5, TimeUnit.SECONDS));
                Assert.isTrue("client is closed", pair.client.isClosed());
            }
            finally {
                pair.close();
            }
        });
    }

    private static void testResetFromPeerFiresConnectionReset()
    {
        Assert.test("a reset from the peer is delivered as connectionReset", () -> {
            Pair pair = new Pair();

            try {
                pair.connect();

                /*
                 * reset() blocks while it flushes its unacknowledged queue, and
                 * the event we care about is the one it triggers on the other
                 * end, so drive it from a helper thread.
                 */
                Thread resetter = new Thread(() -> {
                    try {
                        pair.accepted.reset();
                    }
                    catch (IOException xcp) {
                        /* the connection may be torn down as the test ends */
                    }
                });
                resetter.setDaemon(true);
                resetter.start();

                Assert.isTrue("peer observed the reset",
                        pair.clientState.reset.await(AWAIT_MS, TimeUnit.MILLISECONDS));
            }
            finally {
                pair.close();
            }
        });
    }

    private static void testDataListenerFiresOnce()
    {
        Assert.test("an in-order data packet notifies the packet listener", () -> {
            Pair pair = new Pair();
            CountingPacketListener packets = new CountingPacketListener();
            pair.client.addListener(packets);

            try {
                pair.connect();

                /* The listener on the client fires for data the client receives. */
                pair.accepted.getOutputStream().write(new byte[] { 7 });
                pair.accepted.getOutputStream().flush();

                byte[] one = new byte[1];
                pair.client.setSoTimeout(5000);
                readFully(pair.client.getInputStream(), one);

                Assert.isTrue("in-order event arrived",
                        packets.inOrder.await(5, TimeUnit.SECONDS));
                Assert.isTrue("no false out-of-order event",
                        packets.outOfOrderCount.get() == 0);
            }
            finally {
                pair.close();
            }
        });
    }

    private static void readFully(InputStream in, byte[] buffer)
        throws IOException
    {
        int read = 0;

        while (read < buffer.length) {
            int n = in.read(buffer, read, buffer.length - read);

            if (n < 0) {
                throw new IOException("closed after " + read + " bytes");
            }

            read += n;
        }
    }

    private static ReliableSocketProfile fastProfile()
    {
        return new ReliableSocketProfile(
                32, 32, 512, 16, 3, 2, 4, 0, FAST, FAST, FAST);
    }

    /**
     * A connected client/server pair with the client's state listener
     * attached.
     */
    private static class Pair
    {
        final ReliableServerSocket server;
        final ReliableSocket client;
        final RecordingStateListener clientState = new RecordingStateListener();
        ReliableSocket accepted;

        Pair() throws IOException
        {
            server = new ReliableServerSocket(0, 16);
            client = new ReliableSocket(fastProfile());
        }

        void connect() throws IOException
        {
            client.addStateListener(clientState);
            client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                           AWAIT_MS);
            accepted = (ReliableSocket) server.accept();
        }

        void close()
        {
            closeQuietly(client);

            if (accepted != null) {
                closeQuietly(accepted);
            }

            server.close();
        }
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

    private static class RecordingStateListener implements ReliableSocketStateListener
    {
        final CountDownLatch opened = new CountDownLatch(1);
        final CountDownLatch closed = new CountDownLatch(1);
        final CountDownLatch reset = new CountDownLatch(1);
        final CountDownLatch failure = new CountDownLatch(1);
        final AtomicInteger openedCount = new AtomicInteger();

        public void connectionOpened(ReliableSocket sock)
        {
            openedCount.incrementAndGet();
            opened.countDown();
        }

        public void connectionRefused(ReliableSocket sock) { }

        public void connectionClosed(ReliableSocket sock)
        {
            closed.countDown();
        }

        public void connectionFailure(ReliableSocket sock)
        {
            failure.countDown();
        }

        public void connectionReset(ReliableSocket sock)
        {
            reset.countDown();
        }
    }

    private static class CountingPacketListener implements ReliableSocketListener
    {
        final CountDownLatch inOrder = new CountDownLatch(1);
        final AtomicInteger outOfOrderCount = new AtomicInteger();

        public void packetSent() { }
        public void packetRetransmitted() { }
        public void packetReceivedInOrder()
        {
            inOrder.countDown();
        }
        public void packetReceivedOutOfOrder()
        {
            outOfOrderCount.incrementAndGet();
        }
    }
}
