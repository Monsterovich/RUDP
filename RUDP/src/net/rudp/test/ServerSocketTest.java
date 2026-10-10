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
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import net.rudp.PacketSink;
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.impl.Segment;

/**
 * Tests the server socket's accept contract and how it shares its underlying
 * datagram socket.
 * <p>
 * The sharing rule is the one with a reason behind it: a
 * ReliableServerSocket built on a DatagramSocket the application also uses for
 * outgoing multiplexed connections must not close that socket when the server
 * itself is closed, or every such connection loses its wire. The socket is
 * closed only once the last route is gone - which is a state nothing else
 * observes, so it is asserted here directly.
 */
public class ServerSocketTest
{
    private static final int AWAIT_MS = 10000;

    public static void main(String[] args)
    {
        Assert.suite("server socket");

        testDefaults();
        testSoTimeoutValidation();
        testAcceptTimesOut();
        testAcceptAfterClose();
        testSharedSocketLifetime();
        testAcceptsConnections();

        System.exit(Assert.report());
    }

    private static void testDefaults()
    {
        Assert.test("a fresh server is bound, open and has a port", () -> {
            ReliableServerSocket server = new ReliableServerSocket(0, 16);

            try {
                Assert.isTrue("bound", server.isBound());
                Assert.isFalse("closed", server.isClosed());
                Assert.isTrue("local port assigned", server.getLocalPort() > 0);
                Assert.equals("default so-timeout", 0, server.getSoTimeout());
                Assert.notNull("underlying socket", server.getUnderlyingSocket());
            }
            finally {
                server.close();
            }
        });
    }

    private static void testSoTimeoutValidation()
    {
        Assert.test("a negative accept timeout is rejected", () -> {
            ReliableServerSocket server = new ReliableServerSocket(0, 16);

            try {
                Assert.throwsExactly("setSoTimeout", IllegalArgumentException.class,
                        () -> server.setSoTimeout(-1));

                server.setSoTimeout(250);
                Assert.equals("so-timeout", 250, server.getSoTimeout());
            }
            finally {
                server.close();
            }
        });
    }

    private static void testAcceptTimesOut()
    {
        Assert.test("accept gives up after its timeout when nobody connects", () -> {
            ReliableServerSocket server = new ReliableServerSocket(0, 16);
            server.setSoTimeout(300);

            try {
                long start = System.currentTimeMillis();

                Assert.throwsExactly("accept", SocketTimeoutException.class,
                        () -> server.accept());

                long elapsed = System.currentTimeMillis() - start;

                Assert.isTrue("waited at least the timeout (" + elapsed + "ms)", elapsed >= 250);
            }
            finally {
                server.close();
            }
        });
    }

    private static void testAcceptAfterClose()
    {
        Assert.test("accept on a closed server fails at once", () -> {
            ReliableServerSocket server = new ReliableServerSocket(0, 16);
            server.close();

            Assert.isTrue("closed", server.isClosed());

            Assert.throwsExactly("accept", SocketException.class, () -> server.accept());

            /* close is idempotent. */
            server.close();
            Assert.isTrue("still closed", server.isClosed());
        });
    }

    private static void testSharedSocketLifetime()
    {
        Assert.test("the shared socket outlives the server while a route exists", () -> {
            DatagramSocket shared = new DatagramSocket((SocketAddress) null);
            ReliableServerSocket server = new ReliableServerSocket(shared, 16);
            SocketAddress endpoint = new InetSocketAddress("127.0.0.1", 47001);
            PacketSink sink = (Segment s) -> { };

            server.registerRoute(endpoint, sink);
            Assert.isTrue("route registered", server.checkRoute(endpoint));

            server.close();

            /*
             * The server is closed, but the multiplexed connection behind the
             * route still needs the wire. Closing the shared socket here would
             * silently kill it.
             */
            Assert.isFalse("shared socket kept alive", shared.isClosed());

            server.unregisterRoute(endpoint, sink);

            Assert.isFalse("route removed", server.checkRoute(endpoint));
            Assert.isTrue("shared socket closed once the route is gone", shared.isClosed());
        });

        Assert.test("a route is only removed by its owner", () -> {
            DatagramSocket shared = new DatagramSocket((SocketAddress) null);
            ReliableServerSocket server = new ReliableServerSocket(shared, 16);
            SocketAddress endpoint = new InetSocketAddress("127.0.0.1", 47002);
            PacketSink owner = (Segment s) -> { };
            PacketSink intruder = (Segment s) -> { };

            try {
                server.registerRoute(endpoint, owner);

                /* An unregister from a different sink must not remove it. */
                server.unregisterRoute(endpoint, intruder);
                Assert.isTrue("route survives a foreign unregister", server.checkRoute(endpoint));

                server.unregisterRoute(endpoint, owner);
                Assert.isFalse("owner removed it", server.checkRoute(endpoint));
            }
            finally {
                server.close();
                shared.close();
            }
        });
    }

    private static void testAcceptsConnections()
    {
        Assert.test("accepts two independent connections in order", () -> {
            ReliableServerSocket server = new ReliableServerSocket(0, 16);
            InetSocketAddress endpoint =
                    new InetSocketAddress("127.0.0.1", server.getLocalPort());

            ReliableSocket first = new ReliableSocket();
            ReliableSocket second = new ReliableSocket();
            Socket acceptedFirst = null;
            Socket acceptedSecond = null;

            try {
                first.connect(endpoint, AWAIT_MS);
                second.connect(endpoint, AWAIT_MS);

                Assert.isTrue("first connected", first.isConnected());
                Assert.isTrue("second connected", second.isConnected());

                acceptedFirst = server.accept();
                acceptedSecond = server.accept();

                Assert.notNull("first accepted", acceptedFirst);
                Assert.notNull("second accepted", acceptedSecond);

                /*
                 * Each accepted socket is a live connection of its own: writing
                 * on one and reading it back on the other proves the two are
                 * not the same queue.
                 */
                byte[] message = { 1, 2, 3, 4, 5 };
                acceptedFirst.getOutputStream().write(message);
                acceptedFirst.getOutputStream().flush();

                byte[] read = new byte[message.length];
                readFully(first.getInputStream(), read);
                Assert.arrayEquals("first connection carries its own data", message, read);
            }
            finally {
                closeQuietly(acceptedFirst);
                closeQuietly(acceptedSecond);
                first.close();
                second.close();
                server.close();
            }
        });
    }

    private static void readFully(java.io.InputStream in, byte[] buffer)
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

    private static void closeQuietly(Socket socket)
    {
        if (socket == null) {
            return;
        }

        try {
            socket.close();
        }
        catch (IOException xcp) {
            /* the test is over either way */
        }
    }
}
