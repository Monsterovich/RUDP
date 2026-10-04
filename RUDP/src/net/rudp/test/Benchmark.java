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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketProfile;
import net.rudp.impl.Segment;

/**
 * Performance benchmark for the RUDP library.
 *
 * Measures, over a series of iterations:
 *   1. Throughput    - bulk data transfer in both directions (client->server, server->client).
 *   2. Latency       - round-trip time of many small ping-pong exchanges (min/avg/percentiles/max).
 *   3. Connect rate  - time to establish a connection (handshake latency).
 *   4. Loss resilience - effective throughput when a configurable fraction of UDP datagrams is dropped.
 *
  * Packet loss is simulated by giving the client its own UDP datagram socket
  * that discards a random fraction of its outgoing packets.
  *
  * Usage:
  *   java -cp rudp-SNAPSHOT.jar:src net.rudp.test.Benchmark
  *       [--throughput-N <bytes>] [--latency-N <exchanges>] [--connect-N <conns>]
*       [--loss <fraction 0..1>] [--iterations <n>]
 *   java -cp rudp-SNAPSHOT.jar:src net.rudp.test.Benchmark --help
 */
public class Benchmark
{
    private static final long BYTES_PER_MB = 1024L * 1024L;

    /*
     * Budget for a single read in the throughput runs. These runs have no loss
     * to recover from, so a read that blocks this long means the transfer is
     * stuck: a segment that never comes back and is never retransmitted, or a
     * peer that died mid-run. ReliableSocket.read() waits forever without a
     * timeout, and that turns such a run into one that prints nothing, never
     * exits, and can only be diagnosed by jstack-ing it from another shell.
     * Long enough that a slow machine still finishes a 64 MB payload.
     */
    private static final int THROUGHPUT_IO_TIMEOUT_MS = 30000;

    /*
     * Budget for a single handshake/transfer attempt in the lossy runs. It has
     * to outlast several RTOs with exponential backoff, otherwise a handshake
     * that is merely slow to recover from a dropped SYN is scored as broken.
     */
    private static final int LOSS_CONNECT_TIMEOUT_MS = 20000;
    private static final int LOSS_IO_TIMEOUT_MS = 30000;

    /** Exit code for a command line this benchmark cannot make sense of. */
    private static final int USAGE_EXIT_CODE = 2;

    public static void main(String[] args) throws Exception
    {
        int throughputBytes = 64 * 1024 * 1024;
        int latencyExchanges = 2000;
        int connectCount = 200;
        double lossFraction = 0.0;
        int iterations = 3;

        if (Arrays.asList(args).contains("--help") || Arrays.asList(args).contains("-h")) {
            printUsage();
            return;
        }

        /*
         * Options come in pairs, and a dangling one used to be dropped without
         * a word: the loop ran to args.length - 1, so a trailing "--iterations"
         * changed nothing and the run quietly measured the default instead. A
         * command line this tool cannot parse has to say so and fail, not
         * report a number for something that was never asked for.
         */
        if (args.length % 2 != 0) {
            System.out.println("Every option takes a value, but got " + args.length +
                " arguments: " + String.join(" ", args));
            printUsage();
            System.exit(USAGE_EXIT_CODE);
        }

        for (int i = 0; i < args.length; i += 2) {
            switch (args[i]) {
                case "--throughput-N": throughputBytes = parseInt(args[i + 1]); break;
                case "--latency-N":    latencyExchanges = parseInt(args[i + 1]); break;
                case "--connect-N":    connectCount = parseInt(args[i + 1]); break;
                case "--loss":         lossFraction = Double.parseDouble(args[i + 1]); break;
                case "--iterations":   iterations = parseInt(args[i + 1]); break;
                default:
                    System.out.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(USAGE_EXIT_CODE);
            }
        }

        if (throughputBytes < 1 || latencyExchanges < 1 || connectCount < 1 || iterations < 1) {
            System.out.println("The payload and all counts must be positive.");
            printUsage();
            System.exit(USAGE_EXIT_CODE);
        }

        if (lossFraction < 0.0 || lossFraction > 1.0) {
            System.out.println("--loss is a fraction between 0 and 1, got " + lossFraction + ".");
            printUsage();
            System.exit(USAGE_EXIT_CODE);
        }

        Benchmark bench = new Benchmark();
        System.out.println("RUDP Benchmark");
        System.out.println("  throughput payload : " + throughputBytes + " bytes");
        System.out.println("  latency exchanges  : " + latencyExchanges);
        System.out.println("  connect count      : " + connectCount);
        System.out.println("  loss fraction      : " + format("%.1f%%", lossFraction * 100.0));
        System.out.println("  iterations         : " + iterations + "\n");

        bench.runThroughput(throughputBytes, iterations);
        bench.runLatency(latencyExchanges, iterations);
        bench.runConnect(connectCount, iterations);

        if (lossFraction > 0.0) {
            bench.runLoss(lossFraction, throughputBytes, iterations);
        }

        System.out.println("\n=== Benchmark completed ===");
    }

    private static void printUsage()
    {
        System.out.println(
            "Usage: Benchmark [--throughput-N <bytes>] [--latency-N <exchanges>]\n" +
            "                   [--connect-N <connections>] [--loss <0..1>]\n" +
            "                   [--iterations <n>] [--help]\n" +
            "\n" +
            "  --throughput-N  payload per direction per iteration (default 67108864)\n" +
            "  --latency-N     ping-pong exchanges per iteration (default 2000)\n" +
            "  --connect-N     connections per iteration (default 200)\n" +
            "  --loss          fraction of the client's outgoing datagrams to drop,\n" +
            "                  0 for none (default 0)\n" +
            "  --iterations    repetitions of every phase (default 3)\n" +
            "  --help          print this and exit");
    }

    private static int parseInt(String s) {
        return Integer.parseInt(s.trim());
    }

    // ---------------------------------------------------------------------------
    // Throughput
    // ---------------------------------------------------------------------------

    void runThroughput(final int payloadSize, final int iters) throws Exception
    {
        System.out.println("=== Throughput (measured at receiver) ===");

        long bestClientToServer = 0;
        long bestServerToClient = 0;

        for (int it = 0; it < iters; it++) {
            final CountDownLatch finish = new CountDownLatch(1);
            final double[] stats = new double[2]; // [0] C->S MB/s, [1] S->C MB/s
            final int[] serverPort = new int[1];
            final int[] received = new int[1];
            final IOException[] serverError = new IOException[1];

            Thread server = new Thread(() -> {
                try {
                    ReliableServerSocket serverSocket = new ReliableServerSocket(0);
                    serverPort[0] = serverSocket.getLocalPort();
                    // Bounded accept and read: an iteration whose client never
                    // gets through the handshake, or dies mid-transfer, has to
                    // give up rather than park this thread forever holding the
                    // port for the rest of the run.
                    serverSocket.setSoTimeout(THROUGHPUT_IO_TIMEOUT_MS);
                    Socket client = serverSocket.accept();
                    client.setSoTimeout(THROUGHPUT_IO_TIMEOUT_MS);
                    InputStream in = client.getInputStream();
                    OutputStream out = client.getOutputStream();

                    // Phase 1: client sends payloadSize bytes; server measures receive throughput.
                    byte[] recv = new byte[payloadSize];
                    long start = currentTimeMillis();
                    int total = 0;
                    while (total < payloadSize) {
                        int len = in.read(recv, total, payloadSize - total);
                        if (len == -1) break;
                        total += len;
                    }
                    long elapsed = currentTimeMillis() - start;
                    /*
                     * Credit the bytes that actually arrived, not the payload
                     * size: a transfer cut short - by the read timeout, by a
                     * peer that went away, by EOF - never delivered a full
                     * payload, and dividing the payload size by the time it
                     * took to arrive reports the run as faster than it was.
                     */
                    stats[0] = computeMBps(total, elapsed);
                    received[0] = total;

                    // Phase 2: server sends payloadSize bytes back.
                    start = currentTimeMillis();
                    out.write(randomBytes(payloadSize));
                    out.flush();
                    /*
                     * S->C is timed at the sender, unlike C->S: the write
                     * returns once the payload has been handed to the UDP
                     * socket, so this is "how fast the window filled", not a
                     * receiver-side measurement. Left as it is to keep the
                     * numbers comparable with the pre-optimization baseline.
                     */
                    stats[1] = computeMBps(payloadSize, currentTimeMillis() - start);

                    client.close();
                    serverSocket.close();
                    finish.countDown();
                } catch (IOException e) {
                    serverError[0] = e;
                    finish.countDown();
                }
            });

            server.setDaemon(true);
            server.start();
            Thread.sleep(200);

            IOException clientError = null;
            int receivedBack = 0;

            ReliableSocket client = new ReliableSocket();
            try {
                client.setSoTimeout(THROUGHPUT_IO_TIMEOUT_MS);
                client.connect(new InetSocketAddress("127.0.0.1", serverPort[0]), 5000);
                OutputStream cout = client.getOutputStream();
                InputStream cin = client.getInputStream();
                byte[] drain = new byte[payloadSize];

                // Phase 1: client sends; the server reads it back and measures C->S.
                cout.write(randomBytes(payloadSize));
                cout.flush();

                /*
                 * Phase 2: the server answers with a payload of its own and this
                 * read is what consumes it. There is nothing after it but the FIN
                 * the server sends when it closes, so this used to be followed by
                 * a second readFully that could only ever return 0 bytes at EOF -
                 * the client was in fact timing the close, not a transfer.
                 */
                receivedBack = readFully(cin, drain);
            }
            catch (IOException xcp) {
                /*
                 * A transfer that never finished is a failed attempt, not a
                 * reason to abandon the whole run: report it and let the loop
                 * move on to the next iteration.
                 */
                clientError = xcp;
            }
            finally {
                client.close();
            }

            finish.await(THROUGHPUT_IO_TIMEOUT_MS + 5000, TimeUnit.MILLISECONDS);
            server.join(2000);

            if (clientError != null || serverError[0] != null) {
                System.out.println("Iteration " + it + ": failed (" +
                    errorName(clientError != null ? clientError : serverError[0]) +
                    ") - counted as a failed attempt");
                continue;
            }

            System.out.println("Iteration " + it + ": C->S=" + format("%.1f", stats[0]) +
                " MB/s (" + received[0] + "/" + payloadSize + " bytes), S->C=" +
                format("%.1f", stats[1]) + " MB/s (" + receivedBack + "/" + payloadSize +
                " bytes received by the client)");

            /*
             * Only an iteration that moved the whole payload both ways counts
             * towards the best numbers; a partial one would otherwise be
             * scored on the elapsed time of a transfer that stopped early.
             */
            if (received[0] == payloadSize && receivedBack == payloadSize) {
                bestClientToServer = Math.max(bestClientToServer, (long) stats[0]);
                bestServerToClient = Math.max(bestServerToClient, (long) stats[1]);
            }
        }

        System.out.println("Throughput (best): C->S=" + bestClientToServer + " MB/s, S->C=" + bestServerToClient + " MB/s\n");
    }

    // ---------------------------------------------------------------------------
    // Latency
    // ---------------------------------------------------------------------------

    void runLatency(final int exchanges, final int iters) throws Exception
    {
        System.out.println("=== Latency (ping-pong) ===");

        /*
         * One slot per (iteration, exchange) so that the samples of every
         * iteration feed the aggregate instead of each iteration overwriting
         * the previous one's.
         */
        double[] allRtt = new double[exchanges * iters];
        long totalRtt = 0;
        long bestRtt = Long.MAX_VALUE;

        for (int it = 0; it < iters; it++) {
            final CountDownLatch finish = new CountDownLatch(1);
            final long[] counts = new long[2];
            final int[] serverPort = new int[1];

            Thread server = new Thread(() -> {
                try {
                    ReliableServerSocket serverSocket = new ReliableServerSocket(0);
                    serverPort[0] = serverSocket.getLocalPort();
                    Socket client = serverSocket.accept();
                    InputStream in = client.getInputStream();
                    OutputStream out = client.getOutputStream();
                    byte[] msg = new byte[8];
                    byte[] echo = new byte[8];
                    for (int i = 0; i < exchanges; i++) {
                        readFully(in, msg);
                        out.write(msg);
                        out.flush();
                        counts[0]++;
                    }
                    client.close();
                    serverSocket.close();
                    finish.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                    finish.countDown();
                }
            });

            server.setDaemon(true);
            server.start();
            Thread.sleep(200);

            ReliableSocket client = new ReliableSocket();
            client.connect(new InetSocketAddress("127.0.0.1", serverPort[0]), 5000);
            OutputStream cout = client.getOutputStream();
            InputStream cin = client.getInputStream();
            byte[] msg = new byte[8];
            byte[] resp = new byte[8];

            Random rng = new Random(42L);
            for (int i = 0; i < exchanges; i++) {
                rng.nextBytes(msg);
                long t0 = currentTimeMillis();
                cout.write(msg);
                cout.flush();
                readFully(cin, resp);
                long rtt = currentTimeMillis() - t0;
                allRtt[it * exchanges + i] = rtt;
                totalRtt += rtt;
                if (rtt < bestRtt) bestRtt = rtt;
            }

            client.close();
            finish.await(15, TimeUnit.SECONDS);
            server.join(2000);
        }

        double avg = totalRtt / (double) allRtt.length;
        Arrays.sort(allRtt);
        System.out.println(format(
            "Latency (%d samples over %d iterations): min=%dms avg=%.2fms p50=%.2fms p95=%.2fms p99=%.2fms max=%dms\n",
            allRtt.length, iters, bestRtt, avg,
            percentile(allRtt, 0.50), percentile(allRtt, 0.95), percentile(allRtt, 0.99),
            (long) allRtt[allRtt.length - 1]));
    }

    // ---------------------------------------------------------------------------
    // Connect rate / handshake latency
    // ---------------------------------------------------------------------------

    void runConnect(final int count, final int iters) throws Exception
    {
        System.out.println("=== Connect (handshake) ===");

        long totalConnect = 0;
        long bestConnect = Long.MAX_VALUE;
        int successful = 0;

        for (int it = 0; it < iters; it++) {
            final CountDownLatch finish = new CountDownLatch(1);
            final int[] serverPort = new int[1];

            Thread server = new Thread(() -> {
                try {
                    ReliableServerSocket serverSocket = new ReliableServerSocket(0);
                    serverPort[0] = serverSocket.getLocalPort();
                    for (int i = 0; i < count; i++) {
                        Socket c = serverSocket.accept();
                        /*
                         * Drain until EOF rather than closing right away.
                         * Closing here sends a FIN that can reach the client
                         * while it is still inside connect(), which wakes it
                         * in CLOSE_WAIT and it reports "Socket closed" -- a
                         * failed handshake that was actually established. The
                         * close is driven by the client so that this phase
                         * measures the handshake and nothing else.
                         */
                        InputStream in = c.getInputStream();
                        byte[] scratch = new byte[256];
                        while (in.read(scratch) != -1) {
                            /* discard */
                        }
                        c.close();
                    }
                    serverSocket.close();
                    finish.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                    finish.countDown();
                }
            });

            server.setDaemon(true);
            server.start();
            Thread.sleep(50);

            for (int i = 0; i < count; i++) {
                ReliableSocket client = new ReliableSocket();
                long t0 = currentTimeMillis();
                try {
                    client.connect(new InetSocketAddress("127.0.0.1", serverPort[0]), 5000);
                    long dt = currentTimeMillis() - t0;
                    totalConnect += dt;
                    successful++;
                    if (dt < bestConnect) bestConnect = dt;
                } catch (java.net.SocketException e) {
                    // connection failed; move on to the next attempt
                } finally {
                    client.close();
                }
            }

            finish.await(10, TimeUnit.SECONDS);
            server.join(2000);
        }

        double avg = successful > 0 ? totalConnect / (double) successful : 0;
        System.out.println(format(
            "Connect (%d attempts over %d iterations, %d ok): total=%dms best=%dms avg=%.2fms/connect\n",
            count * iters, iters, successful, totalConnect, bestConnect, avg));
    }

    // ---------------------------------------------------------------------------
    // Packet-loss resilience
    // ---------------------------------------------------------------------------

    void runLoss(final double loss, final int payloadSize, final int iters) throws Exception
    {
        System.out.println("=== Loss resilience (loss=" + format("%.1f%%", loss * 100.0) + ") ===");

        long bestLossy = 0;
        int completed = 0;
        long sentTotal = 0;
        long bytesTotal = 0;

        for (int it = 0; it < iters; it++) {
            final CountDownLatch finish = new CountDownLatch(1);
            final double[] mbps = new double[1];
            final int[] serverPort = new int[1];
            final int[] received = new int[1];
            final IOException[] serverError = new IOException[1];

            Thread server = new Thread(() -> {
                try {
                    ReliableServerSocket serverSocket = new ReliableServerSocket(0);
                    serverPort[0] = serverSocket.getLocalPort();
                    // Bounded accept and read: if this iteration's client never
                    // gets through the handshake, or dies mid-transfer, this
                    // thread must give up instead of leaking and holding the
                    // port for the rest of the run.
                    serverSocket.setSoTimeout(LOSS_IO_TIMEOUT_MS);
                    Socket client = serverSocket.accept();
                    client.setSoTimeout(LOSS_IO_TIMEOUT_MS);
                    InputStream in = client.getInputStream();
                    byte[] recv = new byte[payloadSize];
                    int total = 0;
                    long start = currentTimeMillis();
                    while (total < payloadSize) {
                        int len = in.read(recv, total, payloadSize - total);
                        if (len == -1) break;
                        total += len;
                    }
                    long elapsed = currentTimeMillis() - start;
                    // Credit the bytes that actually arrived, not the payload
                    // size: a transfer cut short by a timeout is not a full
                    // payload delivered.
                    mbps[0] = computeMBps(total, elapsed);
                    received[0] = total;
                    client.close();
                    serverSocket.close();
                    finish.countDown();
                } catch (IOException e) {
                    // A handshake that never completed is an expected outcome at
                    // a non-zero loss rate, so report it instead of dumping a
                    // stack trace over the benchmark output.
                    serverError[0] = e;
                    finish.countDown();
                }
            });

            server.setDaemon(true);
            server.start();
            Thread.sleep(200);

            // Wrap the client's UDP socket with a lossy filter. Bind to an ephemeral
            // local port so it does not collide with the server's listening port.
            LossyDatagramSocket udp = new LossyDatagramSocket();
            udp.bind(new InetSocketAddress("127.0.0.1", 0));
            udp.setLossFraction(loss);

            /*
             * A lossy path can drop the SYN or its acknowledgement, in which
             * case the handshake legitimately fails. That is a measurement of
             * the protocol, not a broken benchmark, so a failed attempt is
             * counted and the loop moves on instead of aborting the whole run.
             */
            IOException clientError = null;
            try {
                ReliableSocket client = new ReliableSocket(udp);
                try {
                    client.connect(new InetSocketAddress("127.0.0.1", serverPort[0]), LOSS_CONNECT_TIMEOUT_MS);
                    OutputStream out = client.getOutputStream();
                    out.write(randomBytes(payloadSize));
                    out.flush();
                }
                finally {
                    client.close();
                }
            }
            catch (IOException e) {
                clientError = e;
            }

            finish.await(LOSS_IO_TIMEOUT_MS + 5000, TimeUnit.MILLISECONDS);
            server.join(3000);

            if (clientError != null) {
                System.out.println("Iteration " + it + ": handshake failed after " +
                    LOSS_CONNECT_TIMEOUT_MS + "ms (" + clientError.getClass().getSimpleName() +
                    ") - counted as a failed attempt");
            }
            else if (serverError[0] != null) {
                System.out.println("Iteration " + it + ": server gave up (" +
                    serverError[0].getClass().getSimpleName() + ")");
            }
            else if (received[0] == payloadSize) {
                System.out.println("Iteration " + it + ": lossy C->S=" + format("%.1f", mbps[0]) +
                    " MB/s (receiver-side, " + received[0] + "/" + payloadSize + " bytes, " +
                    udp.sentCount() + " datagrams sent for " + received[0] + " payload bytes)");
                bestLossy = Math.max(bestLossy, (long) mbps[0]);
                sentTotal += udp.sentCount();
                bytesTotal += received[0];
                completed++;
            }
            else {
                System.out.println("Iteration " + it + ": incomplete transfer, " +
                    received[0] + "/" + payloadSize + " bytes");
            }
        }

        if (completed == 0) {
            System.out.println("Loss resilience: no iteration completed the full payload\n");
        }
        else {
            /*
             * Throughput alone cannot tell a protocol that recovers well from
             * one that recovers by flooding the path: what the lossy runs are
             * really measuring is how many datagrams the sender had to emit to
             * deliver a payload, so that is reported next to the rate. On a
             * clean path a payload of N segments costs N datagrams and the
             * overhead is the loss rate; anything above that is retransmission
             * that did not have to happen.
             */
            System.out.println("Loss resilience (best throughput): " + bestLossy + " MB/s over " +
                completed + "/" + iters + " iterations");
            int segmentCapacity = ReliableSocketProfile.MAX_SEGMENT_SIZE - Segment.RUDP_HEADER_LEN;
            int segmentsPerPayload = (payloadSize + segmentCapacity - 1) / segmentCapacity;
            long segmentsSent = completed * segmentsPerPayload;
            System.out.println("Loss resilience (sender overhead): " + sentTotal + " datagrams for " +
                bytesTotal + " payload bytes over " + segmentsSent + " segments = " +
                format("%.2f", sentTotal / (double) segmentsSent) +
                " datagrams per segment\n");
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private static double computeMBps(long bytes, long elapsedMillis) {
        if (elapsedMillis <= 0) return 0;
        return (bytes / (double) elapsedMillis) * 1000.0 / BYTES_PER_MB;
    }

    /**
     * Formats a number the same way on every machine.
     * <p>
     * String.format without a locale prints a comma as the decimal separator
     * under ru_RU or de_DE, so the run reports "121,3 MB/s" - which is
     * ambiguous to read and impossible to diff between two runs. benchmark.sh
     * also pins -Duser.language=en, which hides the problem instead of
     * fixing it: anything running this class directly (an IDE, a script, a
     * jshell session) still got commas.
     */
    private static String format(String format, Object... args) {
        return String.format(Locale.ROOT, format, args);
    }

    private static String errorName(IOException e) {
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }

    private static byte[] randomBytes(int size) {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return b;
    }

    /**
     * Reads until the buffer is full or the stream ends.
     *
     * @return the number of bytes actually read, which is short of the buffer
     *         only if the peer closed the connection first.
     */
    private static int readFully(InputStream in, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int len = in.read(buf, total, buf.length - total);
            if (len == -1) break;
            total += len;
        }
        return total;
    }

    private static double percentile(double[] sorted, double q) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(q * sorted.length) - 1;
        idx = Math.max(0, Math.min(idx, sorted.length - 1));
        return sorted[idx];
    }

    private static long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // ---------------------------------------------------------------------------
    // Lossy UDP datagram socket: silently drops outgoing datagrams at a given rate.
    // Used to simulate packet loss on the client's outgoing path. Incoming datagrams
    // are passed through unchanged so ACKs still return and the protocol can recover.
    // ---------------------------------------------------------------------------
    private static final class LossyDatagramSocket extends DatagramSocket {
        private volatile double lossFraction = 0.0;
        private final Random rng = new Random();
        private final AtomicInteger sent = new AtomicInteger();

        LossyDatagramSocket() throws IOException {
            super((SocketAddress) null);
        }

        void setLossFraction(double f) {
            this.lossFraction = f;
        }

        /**
         * How many datagrams this socket was handed, dropped ones included.
         * The count is taken before the drop so that what it measures is the
         * work the protocol asked for, not what the path happened to carry.
         */
        int sentCount() {
            return sent.get();
        }

        @Override
        public void send(DatagramPacket p) throws IOException {
            sent.incrementAndGet();

            if (lossFraction > 0.0 && rng.nextDouble() < lossFraction) {
                return; // silently drop
            }

            super.send(p);
        }
    }
}
