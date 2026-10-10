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
import java.net.SocketAddress;
import java.net.SocketException;

import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketListener;
import net.rudp.ReliableSocketStateListener;

/**
 * Tests the java.net.Socket contract on a socket that is not connected.
 * <p>
 * Most of these are the checks callers actually run into: asking an
 * unconnected socket for its streams, reading an address it does not have yet,
 * or shutting down a direction that was never started. They are cheap and they
 * are the behavior a caller sees before it ever gets a connection, so a
 * regression here is a compile-time-looking failure at the call site rather
 * than something deeper.
 * <p>
 * The socket is unconnected throughout, so nothing here needs a peer, a clock
 * or a thread to coordinate with.
 */
public class SocketApiTest
{
    public static void main(String[] args)
    {
        Assert.suite("unconnected socket api");

        testUnconnectedState();
        testStreamsBeforeConnect();
        testUnboundSocket();
        testOptions();
        testShutdownBeforeConnect();
        testListeners();
        testConnectArgumentValidation();
        testCloseContract();

        System.exit(Assert.report());
    }

    private static void testUnconnectedState()
    {
        Assert.test("a fresh socket is bound but not connected", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.isFalse("connected", sock.isConnected());
                Assert.isFalse("closed", sock.isClosed());
                Assert.isTrue("bound", sock.isBound());
                Assert.isTrue("a local port was assigned", sock.getLocalPort() > 0);
            }
            finally {
                sock.close();
            }
        });

        Assert.test("a socket without a peer has no remote address", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.equals("getPort", 0, sock.getPort());
                Assert.equals("getRemoteSocketAddress", null, sock.getRemoteSocketAddress());
                Assert.equals("getInetAddress", null, sock.getInetAddress());
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testStreamsBeforeConnect()
    {
        Assert.test("the streams cannot be had before the connection exists", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.throwsExactly("getInputStream", SocketException.class,
                        () -> sock.getInputStream());
                Assert.throwsExactly("getOutputStream", SocketException.class,
                        () -> sock.getOutputStream());
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testUnboundSocket()
    {
        Assert.test("a socket attached to an unbound datagram socket reports unbound", () -> {
            ReliableSocket sock = new ReliableSocket(new DatagramSocket((SocketAddress) null));

            try {
                Assert.isFalse("bound", sock.isBound());
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testOptions()
    {
        Assert.test("the unsupported socket option is refused", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.throwsExactly("setTcpNoDelay", SocketException.class,
                        () -> sock.setTcpNoDelay(true));
                Assert.isFalse("getTcpNoDelay", sock.getTcpNoDelay());
            }
            finally {
                sock.close();
            }
        });

        Assert.test("keep-alive can be toggled before the connection", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.isTrue("default", sock.getKeepAlive());

                sock.setKeepAlive(false);
                Assert.isFalse("off", sock.getKeepAlive());

                sock.setKeepAlive(true);
                Assert.isTrue("back on", sock.getKeepAlive());

                /* Setting it to what it already is is a no-op, not an error. */
                sock.setKeepAlive(true);
                Assert.isTrue("still on", sock.getKeepAlive());
            }
            finally {
                sock.close();
            }
        });

        Assert.test("the buffer sizes are validated", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.throwsExactly("send size zero", IllegalArgumentException.class,
                        () -> sock.setSendBufferSize(0));
                Assert.throwsExactly("send size negative", IllegalArgumentException.class,
                        () -> sock.setSendBufferSize(-1));
                Assert.throwsExactly("receive size zero", IllegalArgumentException.class,
                        () -> sock.setReceiveBufferSize(0));

                sock.setSendBufferSize(4096);
                Assert.equals("send buffer size", 4096, sock.getSendBufferSize());

                sock.setReceiveBufferSize(8192);
                Assert.equals("receive buffer size", 8192, sock.getReceiveBufferSize());
            }
            finally {
                sock.close();
            }
        });

        Assert.test("a negative so-timeout is rejected", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.throwsExactly("setSoTimeout", IllegalArgumentException.class,
                        () -> sock.setSoTimeout(-1));

                sock.setSoTimeout(1000);
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testShutdownBeforeConnect()
    {
        Assert.test("neither direction can be shutdown before the connection", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.isFalse("input shutdown", sock.isInputShutdown());
                Assert.isFalse("output shutdown", sock.isOutputShutdown());

                Assert.throwsExactly("shutdownInput", SocketException.class,
                        () -> sock.shutdownInput());
                Assert.throwsExactly("shutdownOutput", SocketException.class,
                        () -> sock.shutdownOutput());
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testListeners()
    {
        Assert.test("listeners can be added, removed and removed twice", () -> {
            ReliableSocket sock = new ReliableSocket();
            ReliableSocketListener listener = new ReliableSocketListener() {
                public void packetSent() { }
                public void packetRetransmitted() { }
                public void packetReceivedInOrder() { }
                public void packetReceivedOutOfOrder() { }
            };
            ReliableSocketStateListener stateListener = new ReliableSocketStateListener() {
                public void connectionOpened(ReliableSocket s) { }
                public void connectionRefused(ReliableSocket s) { }
                public void connectionClosed(ReliableSocket s) { }
                public void connectionFailure(ReliableSocket s) { }
                public void connectionReset(ReliableSocket s) { }
            };

            try {
                sock.addListener(listener);
                sock.addListener(listener);      /* idempotent */
                sock.removeListener(listener);
                sock.removeListener(listener);   /* harmless */

                sock.addStateListener(stateListener);
                sock.addStateListener(stateListener);
                sock.removeStateListener(stateListener);
                sock.removeStateListener(stateListener);
            }
            finally {
                sock.close();
            }
        });

        Assert.test("a null listener is rejected", () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.throwsExactly("addListener", NullPointerException.class,
                        () -> sock.addListener(null));
                Assert.throwsExactly("removeListener", NullPointerException.class,
                        () -> sock.removeListener(null));
                Assert.throwsExactly("addStateListener", NullPointerException.class,
                        () -> sock.addStateListener(null));
                Assert.throwsExactly("removeStateListener", NullPointerException.class,
                        () -> sock.removeStateListener(null));
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testConnectArgumentValidation()
    {
        Assert.test("connect rejects a null address, a negative timeout and a foreign type",
                () -> {
            ReliableSocket sock = new ReliableSocket();

            try {
                Assert.throwsExactly("null address", IllegalArgumentException.class,
                        () -> sock.connect(null));
                Assert.throwsExactly("negative timeout", IllegalArgumentException.class,
                        () -> sock.connect(new InetSocketAddress("127.0.0.1", 1), -1));
                Assert.throwsExactly("foreign address type", IllegalArgumentException.class,
                        () -> sock.connect(new SocketAddress() { }));
            }
            finally {
                sock.close();
            }
        });
    }

    private static void testCloseContract()
    {
        Assert.test("close is idempotent and takes the streams with it", () -> {
            ReliableSocket sock = new ReliableSocket();

            Assert.isFalse("closed before", sock.isClosed());

            sock.close();
            Assert.isTrue("closed", sock.isClosed());

            /* A second close must be a no-op rather than an error. */
            sock.close();
            Assert.isTrue("still closed", sock.isClosed());

            Assert.throwsExactly("getInputStream after close", SocketException.class,
                    () -> sock.getInputStream());
            Assert.throwsExactly("getOutputStream after close", SocketException.class,
                    () -> sock.getOutputStream());
            Assert.throwsExactly("shutdownInput after close", SocketException.class,
                    () -> sock.shutdownInput());
            Assert.throwsExactly("shutdownOutput after close", SocketException.class,
                    () -> sock.shutdownOutput());
        });

        Assert.test("a connect on a closed socket is refused", () -> {
            ReliableSocket sock = new ReliableSocket();
            sock.close();

            Assert.throwsExactly("connect", SocketException.class,
                    () -> sock.connect(new InetSocketAddress("127.0.0.1", 1)));
        });
    }
}
