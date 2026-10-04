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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;

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
  */
public class Benchmark
{
    private static final long BYTES_PER_MB = 1024L * 1024L;

    /*
     * Budget for a single handshake/transfer attempt in the lossy runs. It has
     * to outlast several RTOs with exponential backoff, otherwise a handshake
     * that is merely slow to recover from a dropped SYN is scored as broken.
     */
    private static final int LOSS_CONNECT_TIMEOUT_MS = 20000;
    private static final int LOSS_IO_TIMEOUT_MS = 30000;

    public static void main(String[] args) throws Exception
    {
        int throughputBytes = 64 * 1024 * 1024;
        int latencyExchanges = 2000;
        int connectCount = 200;
        double lossFraction = 0.0;
        int iterations = 3;

        for (int i = 0; i < args.length - 1; i += 2) {
            switch (args[i]) {
                case "--throughput-N": throughputBytes = parseInt(args[i + 1]); break;
                case "--latency-N":    latencyExchanges = parseInt(args[i + 1]); break;
                case "--connect-N":    connectCount = parseInt(args[i + 1]); break;
                case "--loss":         lossFraction = Double.parseDouble(args[i + 1]); break;
                case "--iterations":   iterations = parseInt(args[i + 1]); break;
                default:
                    System.out.println("Unknown argument: " + args[i]);
                    return;
            }
        }

        Benchmark bench = new Benchmark();
        System.out.println("RUDP Benchmark");
        System.out.println("  throughput payload : " + throughputBytes + " bytes");
        System.out.println("  latency exchanges  : " + latencyExchanges);
        System.out.println("  connect count      : " + connectCount);
        System.out.println("  loss fraction      : " + String.format("%.1f%%", lossFraction * 100.0));
        System.out.println("  iterations         : " + iterations + "\n");

        bench.runThroughput(throughputBytes, iterations);
        bench.runLatency(latencyExchanges, iterations);
        bench.runConnect(connectCount, iterations);

        if (lossFraction > 0.0) {
            bench.runLoss(lossFraction, throughputBytes, iterations);
        }

        System.out.println("\n=== Benchmark completed ===");
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

            Thread server = new Thread(() -> {
                try {
                    ReliableServerSocket serverSocket = new ReliableServerSocket(0);
                    serverPort[0] = serverSocket.getLocalPort();
                    Socket client = serverSocket.accept();
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
                    stats[0] = computeMBps(payloadSize, start);

                    // Phase 2: server sends payloadSize bytes back; client measures receive throughput.
                    start = currentTimeMillis();
                    out.write(randomBytes(payloadSize));
                    out.flush();
                    stats[1] = computeMBps(payloadSize, start);

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
            byte[] drain = new byte[payloadSize];

            // Phase 1: client sends, server receives.
            cout.write(randomBytes(payloadSize));
            cout.flush();
            readFully(cin, drain);

            // Phase 2: server sends, client receives.
            readFully(cin, drain);

            client.close();
            finish.await(10, java.util.concurrent.TimeUnit.SECONDS);
            server.join(2000);

            System.out.println("Iteration " + it + ": C->S=" + String.format("%.1f", stats[0]) +
                " MB/s, S->C=" + String.format("%.1f", stats[1]) + " MB/s");
            bestClientToServer = Math.max(bestClientToServer, (long) stats[0]);
            bestServerToClient = Math.max(bestServerToClient, (long) stats[1]);
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
            finish.await(15, java.util.concurrent.TimeUnit.SECONDS);
            server.join(2000);
        }

        double avg = totalRtt / (double) allRtt.length;
        Arrays.sort(allRtt);
        System.out.println(String.format(
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

            finish.await(10, java.util.concurrent.TimeUnit.SECONDS);
            server.join(2000);
        }

        double avg = successful > 0 ? totalConnect / (double) successful : 0;
        System.out.println(String.format(
            "Connect (%d attempts over %d iterations, %d ok): total=%dms best=%dms avg=%.2fms/connect\n",
            count * iters, iters, successful, totalConnect, bestConnect, avg));
    }

    // ---------------------------------------------------------------------------
    // Packet-loss resilience
    // ---------------------------------------------------------------------------

    void runLoss(final double loss, final int payloadSize, final int iters) throws Exception
    {
        System.out.println("=== Loss resilience (loss=" + String.format("%.1f%%", loss * 100.0) + ") ===");

        long bestLossy = 0;
        int completed = 0;

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
                    mbps[0] = elapsed > 0 ? (total / (double) elapsed) * 1000.0 / BYTES_PER_MB : 0;
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

            finish.await(LOSS_IO_TIMEOUT_MS + 5000, java.util.concurrent.TimeUnit.MILLISECONDS);
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
                System.out.println("Iteration " + it + ": lossy C->S=" + String.format("%.1f", mbps[0]) +
                    " MB/s (receiver-side, " + received[0] + "/" + payloadSize + " bytes)");
                bestLossy = Math.max(bestLossy, (long) mbps[0]);
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
            System.out.println("Loss resilience (best throughput): " + bestLossy + " MB/s over " +
                completed + "/" + iters + " iterations\n");
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private double computeMBps(int bytes, long startMillis) {
        long elapsed = currentTimeMillis() - startMillis;
        if (elapsed <= 0) return 0;
        return (bytes / (double) elapsed) * 1000.0 / BYTES_PER_MB;
    }

    private static byte[] randomBytes(int size) {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return b;
    }

    private static void readFully(InputStream in, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int len = in.read(buf, total, buf.length - total);
            if (len == -1) break;
            total += len;
        }
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

        LossyDatagramSocket() throws IOException {
            super((SocketAddress) null);
        }

        void setLossFraction(double f) {
            this.lossFraction = f;
        }

        @Override
        public void send(DatagramPacket p) throws IOException {
            if (lossFraction > 0.0 && rng.nextDouble() < lossFraction) {
                return; // silently drop
            }
            super.send(p);
        }
    }
}
