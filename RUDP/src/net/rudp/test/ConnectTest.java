package net.rudp.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;

/**
 * Tests connecting over a datagram socket the application supplied.
 * <p>
 * The socket is supplied unbound, which is what makes these cases worth having.
 * DatagramSocket.receive() is synchronized and holds the socket's own monitor
 * for as long as it waits for a datagram, and so is bind() - so on an unbound
 * socket the reader thread binds it as it enters receive(), and any thread that
 * means to bind it itself has to win the monitor first:
 * <ul>
 * <li>DatagramSocket.send() binds an unbound socket on the way out, after
 *     reading isBound(). If the reader takes the monitor between that read and
 *     the bind, it binds the socket itself and parks holding the monitor. The
 *     sender is then stuck inside bind() waiting for a datagram that cannot
 *     arrive, because the SYN that would draw one is the very thing stuck
 *     behind that bind.</li>
 * <li>bind() called by the application needs the same monitor for as long as
 *     the reader is parked, which is every moment until the peer answers - and
 *     nothing can make the peer answer, so this one does not even need a
 *     race.</li>
 * </ul>
 * Both were fixed by having the reader wait for the socket to be bound before
 * it parks in receive(), so a monitor it holds is never one a bind is waiting
 * for. The second case below is the deterministic guard; the first is the
 * racy one that made a fresh connection hang or work depending on the machine.
 */
public class ConnectTest
{
    /**
     * Long enough for the handshake, short enough to bound a broken run. The
     * wait is real time and it is compared against the socket's own clock,
     * which here is the system one, so a stalled handshake is reported as a
     * SocketTimeoutException.
     */
    private static final int CONNECT_TIMEOUT_MS = 10000;

    /**
     * How long to wait for the reader thread to reach receive() before the
     * first send. Nothing in these cases depends on the two racing: without the
     * wait, whether the case passes or hangs would be a coin flip, and the bug
     * it covers is a hang rather than a wrong answer, so it has to be provoked
     * rather than hoped for. A thread needs microseconds to get from start() to
     * a blocking recvfrom(), so this is orders of magnitude more than enough.
     */
    private static final int READER_PARKED_MS = 250;

    private static final String MESSAGE = "Hello, RUDP!";

    public static void main(String[] args)
    {
        Assert.suite("connection setup");

        testConnectsOverUnboundSocket();
        testBindsAfterTheSocketWasCreated();

        System.exit(Assert.report());
    }

    /**
     * The shape that hung: the reader thread is already parked on an unbound
     * socket when the application sends the SYN.
     */
    private static void testConnectsOverUnboundSocket()
    {
        Assert.test("connects over an unbound socket the reader is parked on", () -> {
            Peer peer = new Peer();
            ReliableServerSocket server = peer.start();

            ReliableSocket client = new ReliableSocket(unboundSocket());

            try {
                sleep(READER_PARKED_MS);

                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                peer.awaitConnection();

                Assert.isTrue("connected", client.isConnected());
                Assert.isTrue("bound to a local port", client.getLocalPort() > 0);
                Assert.equals("the message arrived", MESSAGE, echo(client));
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
            }
        });
    }

    /**
     * The same race reached through the application binding the socket itself,
     * which is what a caller that wants a particular local port does. The bind
     * needs the same monitor the parked reader holds, so it has to survive the
     * same wait - and it has to keep working, since binding inside the library
     * instead would break every caller that binds afterwards.
     */
    private static void testBindsAfterTheSocketWasCreated()
    {
        Assert.test("binds a socket the reader is parked on, then connects", () -> {
            Peer peer = new Peer();
            ReliableServerSocket server = peer.start();

            ReliableSocket client = new ReliableSocket(unboundSocket());

            try {
                sleep(READER_PARKED_MS);

                client.bind(new InetSocketAddress("127.0.0.1", 0));

                Assert.isTrue("bound", client.getLocalSocketAddress() != null);

                client.connect(new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                        CONNECT_TIMEOUT_MS);
                peer.awaitConnection();

                Assert.isTrue("connected", client.isConnected());
                Assert.equals("the message arrived", MESSAGE, echo(client));
            }
            finally {
                closeQuietly(client);
                peer.shutdown();
                server.close();
            }
        });
    }

    private static DatagramSocket unboundSocket()
        throws IOException
    {
        return new DatagramSocket((SocketAddress) null);
    }

    /** Writes the message and reads the peer's echo of it back. */
    private static String echo(Socket client)
        throws IOException
    {
        OutputStream out = client.getOutputStream();
        out.write(MESSAGE.getBytes());
        out.flush();

        return readExactly(client.getInputStream(), MESSAGE.length());
    }

    private static String readExactly(InputStream in, int length)
        throws IOException
    {
        byte[] buffer = new byte[length];
        int read = 0;

        while (read < length) {
            int n = in.read(buffer, read, length - read);

            if (n < 0) {
                throw new IOException("the peer closed after " + read + " of " + length +
                        " bytes");
            }

            read += n;
        }

        return new String(buffer);
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

    private static void closeQuietly(Socket socket)
    {
        try {
            socket.close();
        }
        catch (IOException xcp) {
            /* the test is over either way */
        }
    }

    /** The server end: accepts one connection and echoes one message back. */
    private static final class Peer
    {
        ReliableServerSocket start()
            throws IOException
        {
            _server = new ReliableServerSocket(0, 16);
            _thread = new Thread(this::run, "ConnectTest-Peer");
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
                int len = accepted.getInputStream().read(buffer);

                if (len > 0) {
                    accepted.getOutputStream().write(buffer, 0, len);
                    accepted.getOutputStream().flush();
                }
            }
            catch (IOException xcp) {
                /* the connection was closed under us, which ends the peer */
            }
            finally {
                closeQuietly(accepted);
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
             * ReliableServerSocket.close() deliberately leaves a live client
             * connection alone, so the read() above stays parked and the join
             * would always run out its timeout. Closing the accepted socket is
             * what unblocks it.
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
}