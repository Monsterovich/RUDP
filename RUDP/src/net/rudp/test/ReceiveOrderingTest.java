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
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketListener;
import net.rudp.impl.Segment;

/**
 * A TCP-like stream must hand bytes to the reader in the order they were
 * written, no matter what order they show up in or how often they are
 * repeated. These cases corrupt the datagram order on the receiving side of a
 * live connection and check that the stream still comes out whole and correct.
 * <p>
 * The corruption is applied to the receiving socket rather than through a
 * hand-built peer: a real connection is driven end to end, and only the first
 * data datagram is tampered with, so the reassembly path is what is under test
 * rather than the handshake and acknowledgment machinery the other suites
 * already cover.
 */
public class ReceiveOrderingTest
{
    private static final int AWAIT_MS = 10000;
    private static final int PAYLOAD = 2000;
    private static final int SEG_PAYLOAD = 1200 - Segment.RUDP_HEADER_LEN;

    public static void main(String[] args)
    {
        Assert.suite("receive ordering");

        testInOrderBaseline();
        testOutOfOrderIsReassembled();
        testMissingSegmentIsRecovered();
        testDuplicateIsDeliveredOnce();

        System.exit(Assert.report());
    }

    private static void testInOrderBaseline()
    {
        Assert.test("an undisturbed stream arrives intact and in order", () -> {
            Harness h = new Harness(Mangle.Mode.NONE);
            CountingListener counts = new CountingListener();
            h.client.addListener(counts);

            try {
                h.connect();
                h.reorder.arm();

                byte[] sent = payload();
                h.accepted.getOutputStream().write(sent);
                h.accepted.getOutputStream().flush();

                Assert.arrayEquals("payload", sent, h.read(sent.length));
                Assert.equals("no out-of-order events", 0, counts.outOfOrder.get());
            }
            finally {
                h.close();
            }
        });
    }

    private static void testOutOfOrderIsReassembled()
    {
        Assert.test("a swapped pair of segments is reassembled in order", () -> {
            Harness h = new Harness(Mangle.Mode.SWAP);
            CountingListener counts = new CountingListener();
            h.client.addListener(counts);

            try {
                h.connect();
                h.reorder.arm();

                byte[] sent = payload();
                h.accepted.getOutputStream().write(sent);
                h.accepted.getOutputStream().flush();

                Assert.arrayEquals("payload despite the swap", sent, h.read(sent.length));
                Assert.isTrue("the swap was seen as out of order",
                        counts.outOfOrder.get() > 0);
            }
            finally {
                h.close();
            }
        });
    }

    private static void testMissingSegmentIsRecovered()
    {
        Assert.test("a dropped segment is recovered and the stream is whole", () -> {
            Harness h = new Harness(Mangle.Mode.DROP);
            CountingListener counts = new CountingListener();
            h.client.addListener(counts);

            try {
                h.connect();
                h.reorder.arm();

                byte[] sent = payload();
                h.accepted.getOutputStream().write(sent);
                h.accepted.getOutputStream().flush();

                /*
                 * The drop forces a retransmission; the payload must still be
                 * complete and correctly ordered when it lands.
                 */
                Assert.arrayEquals("payload despite the drop", sent, h.read(sent.length));
                Assert.isTrue("the gap was reported", counts.outOfOrder.get() > 0);
            }
            finally {
                h.close();
            }
        });
    }

    private static void testDuplicateIsDeliveredOnce()
    {
        Assert.test("a duplicated segment is delivered exactly once", () -> {
            Harness h = new Harness(Mangle.Mode.DUPLICATE);

            try {
                h.connect();
                h.reorder.arm();

                byte[] sent = payload();
                h.accepted.getOutputStream().write(sent);
                h.accepted.getOutputStream().flush();

                Assert.arrayEquals("payload with no duplication", sent, h.read(sent.length));
            }
            finally {
                h.close();
            }
        });
    }

    private static byte[] payload()
    {
        byte[] bytes = new byte[PAYLOAD];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i * 31 + 7);
        }

        return bytes;
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

    /**
     * A connected client with a mangling datagram socket and a real server.
     */
    private static class Harness
    {
        final ReliableServerSocket server;
        final Mangle reorder;
        final ReliableSocket client;
        ReliableSocket accepted;

        Harness(Mangle.Mode mode) throws IOException
        {
            server = new ReliableServerSocket(0, 16);
            reorder = new Mangle();
            reorder.mode = mode;
            client = new ReliableSocket(reorder);
        }

        void connect() throws IOException
        {
            client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                           AWAIT_MS);
            accepted = (ReliableSocket) server.accept();
        }

        byte[] read(int length) throws IOException
        {
            byte[] buffer = new byte[length];
            client.setSoTimeout(AWAIT_MS);
            readFully(client.getInputStream(), buffer);
            return buffer;
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

    /**
     * Reorders the first inbound data datagram once armed. Everything before
     * arming - the handshake - and everything after the first data segment
     * passes through untouched.
     */
    private static class Mangle extends DatagramSocket
    {
        enum Mode { NONE, SWAP, DROP, DUPLICATE }

        volatile Mode mode = Mode.NONE;
        volatile boolean armed;

        private int _dataSeen;
        private byte[] _pending;
        private byte[] _held;
        private SocketAddress _heldFrom;
        private boolean _dropDone;

        Mangle() throws IOException
        {
            super((SocketAddress) null);
        }

        void arm()
        {
            armed = true;
        }

        @Override
        public void receive(DatagramPacket p)
            throws IOException
        {
            while (true) {
                if (_held != null) {
                    byte[] taken = _held;
                    _held = null;
                    System.arraycopy(taken, 0, p.getData(), p.getOffset(), taken.length);
                    p.setLength(taken.length);
                    p.setSocketAddress(_heldFrom);
                    return;
                }

                super.receive(p);

                Segment segment = parse(p);

                if (!armed || segment == null || !"DAT".equals(segment.type())) {
                    return;
                }

                _dataSeen++;

                if (_dataSeen != 1) {
                    /*
                     * The second data segment is what triggers the swap: the
                     * one held back becomes ready to return on the next read,
                     * so the reader receives the two out of order.
                     */
                    if (mode == Mode.SWAP && _pending != null) {
                        _held = _pending;
                        _pending = null;
                        _heldFrom = p.getSocketAddress();
                    }

                    return;
                }

                if (mode == Mode.SWAP) {
                    /* Stash the first; it is released after the second. */
                    _pending = copy(p);
                    continue;
                }

                if (mode == Mode.DROP) {
                    if (_dropDone) {
                        return;
                    }

                    /*
                     * Swallow it once. The retransmission carries the same
                     * sequence number and flows through here again.
                     */
                    _dropDone = true;
                    continue;
                }

                if (mode == Mode.DUPLICATE) {
                    /* Return it now, and the same bytes again next time. */
                    _held = copy(p);
                    _heldFrom = p.getSocketAddress();
                    return;
                }

                return;
            }
        }

        private static byte[] copy(DatagramPacket p)
        {
            byte[] bytes = new byte[p.getLength()];
            System.arraycopy(p.getData(), p.getOffset(), bytes, 0, bytes.length);
            return bytes;
        }

        private static Segment parse(DatagramPacket p)
        {
            try {
                return Segment.parse(p.getData(), p.getOffset(), p.getLength());
            }
            catch (RuntimeException xcp) {
                return null;
            }
        }
    }

    private static class CountingListener implements ReliableSocketListener
    {
        final AtomicInteger inOrder = new AtomicInteger();
        final AtomicInteger outOfOrder = new AtomicInteger();

        public void packetSent() { }
        public void packetRetransmitted() { }
        public void packetReceivedInOrder()
        {
            inOrder.incrementAndGet();
        }
        public void packetReceivedOutOfOrder()
        {
            outOfOrder.incrementAndGet();
        }
    }
}
