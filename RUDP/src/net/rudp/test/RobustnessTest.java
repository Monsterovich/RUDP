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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Random;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketProfile;

/**
 * RUDP runs over UDP, so anyone can send this library anything at any time.
 * The dispatcher and the per-connection reader are the only guards against
 * that, and the contract they advertise is narrow: a datagram that is not a
 * well-formed segment is dropped, and dropping it must not take the port or
 * the connection down with it.
 * <p>
 * These cases throw malformed datagrams - random bytes, truncated headers,
 * and empty ones - at both a listening server and a live connection, and
 * check that each keeps working afterwards. A reader thread that died on the
 * first bad packet would leave a connection that still reports itself
 * connected but never delivers another byte, which is exactly what the echo
 * at the end rules out.
 */
public class RobustnessTest
{
    private static final int AWAIT_MS = 10000;
    private static final int FAST = 100;

    public static void main(String[] args)
    {
        Assert.suite("robustness");

        testGarbageDoesNotStopTheServer();
        testGarbageDoesNotStopAConnection();
        testEmptyAndTruncatedDatagrams();
        testBurstOfGarbage();

        System.exit(Assert.report());
    }

    private static void testGarbageDoesNotStopTheServer()
    {
        Assert.test("malformed datagrams to the listening port are dropped", () -> {
            ReliableServerSocket server = new ReliableServerSocket(0, 16);
            DatagramSocket bomber = new DatagramSocket((SocketAddress) null);

            try {
                bomb(bomber, server.getLocalPort(), 40);

                /* The port must still complete a handshake afterwards. */
                ReliableSocket client = new ReliableSocket(fastProfile());
                client.connect(local(server.getLocalPort()), AWAIT_MS);
                Socket accepted = server.accept();

                echo(client, (ReliableSocket) accepted);

                client.close();
                accepted.close();
            }
            finally {
                bomber.close();
                server.close();
            }
        });
    }

    private static void testGarbageDoesNotStopAConnection()
    {
        Assert.test("malformed datagrams to a live connection are ignored", () -> {
            Pair pair = new Pair();

            try {
                pair.connect();

                /* Aim at the client's own port, not the server's. */
                DatagramSocket bomber = new DatagramSocket((SocketAddress) null);
                bomb(bomber, pair.client.getLocalPort(), 40);
                bomber.close();

                Assert.isTrue("still connected", pair.client.isConnected());
                Assert.isFalse("not closed", pair.client.isClosed());

                echo(pair.client, pair.accepted);
            }
            finally {
                pair.close();
            }
        });
    }

    private static void testEmptyAndTruncatedDatagrams()
    {
        Assert.test("empty and truncated datagrams are ignored", () -> {
            Pair pair = new Pair();
            DatagramSocket bomber = new DatagramSocket((SocketAddress) null);

            try {
                pair.connect();

                bomber.send(new DatagramPacket(new byte[0], 0,
                        local(pair.client.getLocalPort())));
                bomber.send(new DatagramPacket(new byte[1], 1,
                        local(pair.client.getLocalPort())));
                bomber.send(new DatagramPacket(new byte[5], 5,
                        local(pair.client.getLocalPort())));

                Assert.isTrue("still connected", pair.client.isConnected());
                echo(pair.client, pair.accepted);
            }
            finally {
                bomber.close();
                pair.close();
            }
        });
    }

    private static void testBurstOfGarbage()
    {
        Assert.test("a burst of junk does not exhaust or wedge the reader", () -> {
            Pair pair = new Pair();

            try {
                pair.connect();

                DatagramSocket bomber = new DatagramSocket((SocketAddress) null);
                bomb(bomber, pair.client.getLocalPort(), 500);
                bomber.close();

                Assert.isTrue("still connected", pair.client.isConnected());
                echo(pair.client, pair.accepted);
            }
            finally {
                pair.close();
            }
        });
    }

    /**
     * Sends a pile of payloads that no parser can mistake for a segment: each
     * is either shorter than the six-byte RUDP header or starts with a flags
     * byte of zero, which selects no segment type at all.
     */
    private static void bomb(DatagramSocket socket, int port, int count)
        throws IOException
    {
        SocketAddress target = local(port);
        Random random = new Random(1);

        for (int i = 0; i < count; i++) {
            int length = 1 + random.nextInt(48);
            byte[] payload = new byte[length];
            random.nextBytes(payload);

            if (length >= 6) {
                /* Clear the flags byte so the segment type is invalid. */
                payload[0] = 0;
            }

            socket.send(new DatagramPacket(payload, payload.length, target));
        }
    }

    private static SocketAddress local(int port)
    {
        return new InetSocketAddress("127.0.0.1", port);
    }

    private static void echo(ReliableSocket sender, ReliableSocket receiver)
        throws IOException
    {
        byte[] message = { 11, 22, 33, 44 };
        sender.getOutputStream().write(message);
        sender.getOutputStream().flush();

        receiver.setSoTimeout(AWAIT_MS);
        byte[] read = new byte[message.length];
        readFully(receiver.getInputStream(), read);
        Assert.arrayEquals("the connection still carries data", message, read);
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

    private static class Pair
    {
        final ReliableServerSocket server;
        final ReliableSocket client;
        ReliableSocket accepted;

        Pair() throws IOException
        {
            server = new ReliableServerSocket(0, 16);
            client = new ReliableSocket(fastProfile());
        }

        void connect() throws IOException
        {
            client.connect(local(server.getLocalPort()), AWAIT_MS);
            accepted = (ReliableSocket) server.accept();
        }

        void close()
        {
            try {
                client.close();
            }
            catch (IOException xcp) {
                /* the test is over either way */
            }

            if (accepted != null) {
                try {
                    accepted.close();
                }
                catch (IOException xcp) {
                    /* the test is over either way */
                }
            }

            server.close();
        }
    }
}
