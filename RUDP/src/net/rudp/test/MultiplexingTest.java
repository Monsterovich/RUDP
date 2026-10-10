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
import java.net.SocketAddress;

import net.rudp.MultiplexedReliableSocket;
import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketProfile;

/**
 * A server can use its own bound datagram socket to make outgoing connections
 * as well as to accept incoming ones. Every such connection is identified by
 * the remote endpoint, and the routing table that keeps them apart is what
 * these cases pin down: a live route must block a duplicate, a closed
 * connection must give its route back, and a hand-registered route must be
 * honored just like an automatic one.
 * <p>
 * Connecting a multiplexed socket does not bind a new port - it reuses the
 * server's - so a second server stands in for the remote side and the route
 * key is that server's address.
 */
public class MultiplexingTest
{
    private static final int AWAIT_MS = 10000;
    private static final int FAST = 100;

    public static void main(String[] args)
    {
        Assert.suite("multiplexing");

        testMultiplexedConnectionCarriesData();
        testDuplicateRouteIsRefused();
        testClosedConnectionReleasesRoute();
        testManualRouteRegistration();

        System.exit(Assert.report());
    }

    private static void testMultiplexedConnectionCarriesData()
    {
        Assert.test("an outgoing connection over the server's socket carries data", () -> {
            Fixture f = new Fixture();

            try {
                SocketAddress remote = f.remoteEndpoint();
                MultiplexedReliableSocket client =
                        new MultiplexedReliableSocket(f.server, fastProfile());

                client.connect(remote, AWAIT_MS);
                ReliableSocket accepted = (ReliableSocket) f.remote.accept();

                Assert.isTrue("route registered", f.server.checkRoute(remote));
                Assert.isTrue("client connected", client.isConnected());

                echo(client, accepted, new byte[] { 10, 20, 30 });

                client.close();
                accepted.close();
            }
            finally {
                f.close();
            }
        });
    }

    private static void testDuplicateRouteIsRefused()
    {
        Assert.test("a second connection to a live endpoint is refused, first survives", () -> {
            Fixture f = new Fixture();

            try {
                SocketAddress remote = f.remoteEndpoint();
                MultiplexedReliableSocket first =
                        new MultiplexedReliableSocket(f.server, fastProfile());
                first.connect(remote, AWAIT_MS);
                ReliableSocket accepted = (ReliableSocket) f.remote.accept();

                MultiplexedReliableSocket second =
                        new MultiplexedReliableSocket(f.server, fastProfile());

                boolean refused = false;
                try {
                    second.connect(remote, AWAIT_MS);
                }
                catch (IOException xcp) {
                    refused = "Already connected".equals(xcp.getMessage());
                }

                Assert.isTrue("duplicate connect refused with the right message", refused);

                /*
                 * The failed attempt must not have torn the first connection
                 * down on its way out.
                 */
                Assert.isTrue("first still connected", first.isConnected());
                echo(first, accepted, new byte[] { 99, 98, 97 });

                first.close();
                second.close();
                accepted.close();
            }
            finally {
                f.close();
            }
        });
    }

    private static void testClosedConnectionReleasesRoute()
    {
        Assert.test("closing a connection releases its route for reuse", () -> {
            Fixture f = new Fixture();

            try {
                SocketAddress remote = f.remoteEndpoint();
                MultiplexedReliableSocket first =
                        new MultiplexedReliableSocket(f.server, fastProfile());
                first.connect(remote, AWAIT_MS);
                ReliableSocket acceptedFirst = (ReliableSocket) f.remote.accept();

                first.close();

                /*
                 * The route is dropped when the close reaches its listener,
                 * which happens after the duplicate-close timer - the fast
                 * profile keeps that short. Poll rather than sleep a fixed
                 * amount.
                 */
                Assert.isTrue("route released",
                        awaitRouteCleared(f.server, remote, AWAIT_MS));

                /*
                 * The remote side still holds the connection it accepted for
                 * this endpoint; until that is closed and released, a new SYN
                 * from the same source address is routed to the dead socket
                 * instead of starting a fresh session. Its cleanup is part of
                 * what makes reuse possible, so wait it out too.
                 */
                acceptedFirst.close();
                SocketAddress clientLocal =
                        new InetSocketAddress("127.0.0.1", f.server.getLocalPort());
                Assert.isTrue("remote released the old session",
                        awaitRouteCleared(f.remote, clientLocal, AWAIT_MS));

                MultiplexedReliableSocket second =
                        new MultiplexedReliableSocket(f.server, fastProfile());
                second.connect(remote, AWAIT_MS);
                ReliableSocket acceptedSecond = (ReliableSocket) f.remote.accept();

                echo(second, acceptedSecond, new byte[] { 5, 6, 7 });

                second.close();
                acceptedSecond.close();
            }
            finally {
                f.close();
            }
        });
    }

    private static void testManualRouteRegistration()
    {
        Assert.test("a hand-registered route lets a multiplexed socket connect", () -> {
            Fixture f = new Fixture();

            try {
                SocketAddress remote = f.remoteEndpoint();

                /*
                 * No server reference, so no automatic registration: the route
                 * must be installed by hand before the SYN-ACK can find its way
                 * back.
                 */
                MultiplexedReliableSocket client =
                        new MultiplexedReliableSocket(f.server.getUnderlyingSocket(),
                                                      fastProfile());
                f.server.registerRoute(remote, client);
                client.connect(remote, AWAIT_MS);
                ReliableSocket accepted = (ReliableSocket) f.remote.accept();

                echo(client, accepted, new byte[] { 1, 1, 1 });

                f.server.unregisterRoute(remote, client);
                client.close();
                accepted.close();
            }
            finally {
                f.close();
            }
        });
    }

    private static boolean awaitRouteCleared(ReliableServerSocket server,
                                             SocketAddress endpoint, int timeoutMs)
    {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            if (!server.checkRoute(endpoint)) {
                return true;
            }

            try {
                Thread.sleep(20);
            }
            catch (InterruptedException xcp) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    private static void echo(ReliableSocket sender, ReliableSocket receiver, byte[] message)
        throws IOException
    {
        sender.getOutputStream().write(message);
        sender.getOutputStream().flush();

        receiver.setSoTimeout(AWAIT_MS);
        byte[] read = new byte[message.length];
        readFully(receiver.getInputStream(), read);
        Assert.arrayEquals("echoed bytes", message, read);
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
     * A server whose socket is used for outgoing connections, and a second
     * server standing in for the remote side.
     */
    private static class Fixture
    {
        final ReliableServerSocket server;
        final ReliableServerSocket remote;

        Fixture() throws IOException
        {
            server = new ReliableServerSocket(0, 16);
            remote = new ReliableServerSocket(0, 16);
        }

        SocketAddress remoteEndpoint()
        {
            return new InetSocketAddress("127.0.0.1", remote.getLocalPort());
        }

        void close()
        {
            server.close();
            remote.close();
        }
    }
}
