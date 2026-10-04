/*
 * Simple Reliable UDP (rudp)
 * Copyright (c) 2009, Adrian Granados (agranados@ihmc.us)
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
 * HOLDERS AND CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.rudp.impl.ACKSegment;
import net.rudp.impl.DATSegment;
import net.rudp.impl.EAKSegment;
import net.rudp.impl.FINSegment;
import net.rudp.impl.NULSegment;
import net.rudp.impl.RSTSegment;
import net.rudp.impl.RtoEstimator;
import net.rudp.impl.SYNSegment;
import net.rudp.impl.Segment;
import net.rudp.impl.Timer;

/**
 * This class implements client sockets that use
 * the Simple Reliable UDP (RUDP) protocol for
 * end-to-end communication between two machines.
 *
 * @author Adrian Granados
 * @see    java.net.Socket
 */
public class ReliableSocket extends Socket
{
    /**
     * Creates an unconnected RUDP socket with default RUDP parameters.
     *
     * @throws IOException if an I/O error occurs when
     *         creating the underlying UDP socket.
     */
    public ReliableSocket()
        throws IOException
    {
        this(new ReliableSocketProfile());
    }

    /**
     * Creates an unconnected RUDP socket and uses the given RUDP parameters.
     *
     * @throws IOException if an I/O error occurs when
     *            creating the underlying UDP socket.
     */
    public ReliableSocket(ReliableSocketProfile profile)
        throws IOException
    {
        this(new DatagramSocket(), profile);
    }

    /**
     * Creates a RUDP socket and connects it to the specified port
     * number on the named host.
     * <p>
     * If the specified host is <tt>null</tt> it is the equivalent of
     * specifying the address as <tt>{@link java.net.InetAddress#getByName InetAddress.getByName}(null)</tt>.
     * In other words, it is equivalent to specifying an address of the
     * loopback interface.
     *
     * @param host the host name, or <code>null</code> for the loopback address.
     * @param port the port number.
     *
     * @throws UnknownHostException if the IP address of the host could not be determined.
     * @throws IOException if an I/O error occurs when creating the socket.
     * @throws IllegalArgumentException if the port parameter is outside the specified range
     *         of valid port values, which is between 0 and 65535, inclusive.
     * @see java.net.Socket#Socket(String, int)
     */
    public ReliableSocket(String host, int port)
        throws UnknownHostException, IOException
    {
        this(new InetSocketAddress(host, port), null);
    }

    /**
     * Creates a RUDP socket and connects it to the specified remote address on
     * the specified remote port. The socket will also bind to the local address
     * and port supplied.
     * <p>
     * If the specified local address is <tt>null</tt> it is the equivalent of
     * specifying the address as the wildcard address
     * (see <tt>{@link java.net.InetAddress#isAnyLocalAddress InetAddress.isAnyLocalAddress}()</tt>).
     * <p>
     * A local port number of <code>zero</code> will let the system pick up a
     * free port in the <code>bind</code> operation.
     *
     * @param address   the remote address.
     * @param port      the remote port.
     * @param localAddr the local address the socket is bound to, or
     *                  <code>null</code> for the wildcard address.
     * @param localPort the local port the socket is bound to, or
     *                  <code>zero</code> for a system selected free port.
     * @throws IOException if an I/O error occurs when creating the socket.
     * @throws IllegalArgumentException if the port parameter is outside the specified range
     *         of valid port values, which is between 0 and 65535, inclusive.
     */
    public ReliableSocket(InetAddress address, int port, InetAddress localAddr, int localPort)
        throws IOException
    {
        this(new InetSocketAddress(address, port),
                new InetSocketAddress(localAddr, localPort));
    }

    /**
     * Creates a RUDP socket and connects it to the specified remote host on
     * the specified remote port. The socket will also bind to the local address
     * and port supplied.
     * <p>
     * If the specified host is <tt>null</tt> it is the equivalent of
     * specifying the address as <tt>{@link java.net.InetAddress#getByName InetAddress.getByName}(null)</tt>.
     * In other words, it is equivalent to specifying an address of the
     * loopback interface.
     * <p>
     * A local port number of <code>zero</code> will let the system pick up a
     * free port in the <code>bind</code> operation.
     *
     * @param host      the name of the remote host, or <code>null</code> for the loopback address.
     * @param port      the remote port.
     * @param localAddr the local address the socket is bound to, or
     *                  <code>null</code> for the wildcard address.
     * @param localPort the local port the socket is bound to, or
     *                  <code>zero</code> for a system selected free port.
     * @throws IOException if an I/O error occurs when creating the socket.
     * @throws IllegalArgumentException if the port parameter is outside the specified range
     *         of valid port values, which is between 0 and 65535, inclusive.
     */
    public ReliableSocket(String host, int port, InetAddress localAddr, int localPort)
        throws IOException
    {
        this(new InetSocketAddress(host, port),
                new InetSocketAddress(localAddr, localPort));
    }

    /**
     * Creates a RUDP socket and connects it to the specified remote address. The
     * socket will also bind to the local address supplied.
     *
     * @param inetAddr  the remote address.
     * @param localAddr the local address.
     * @throws IOException if an I/O error occurs when creating the socket.
     */
    protected ReliableSocket(InetSocketAddress inetAddr, InetSocketAddress localAddr)
        throws IOException
    {
        this(new DatagramSocket(localAddr), new ReliableSocketProfile());
        connect(inetAddr);
    }

    /**
     * Creates a RUDP socket and attaches it to the underlying datagram socket.
     *
     * @param sock the datagram socket.
     */
    public ReliableSocket(DatagramSocket sock)
    {
        this(sock, new ReliableSocketProfile());
    }

    /**
     * Creates a RUDP socket and attaches it to the underlying
     * datagram socket using the given RUDP parameters.
     *
     * @param sock the datagram socket.
     * @param profile the socket profile.
     */
    protected ReliableSocket(DatagramSocket sock, ReliableSocketProfile profile)
    {
        this(sock, profile, Clock.SYSTEM);
    }

    /**
     * Creates a RUDP socket and attaches it to the underlying
     * datagram socket using the given RUDP parameters, taking every
     * timestamp from the given clock.
     * <p>
     * The clock is installed before init() runs, so it is already in place
     * when the socket thread and the timer threads start.
     *
     * @param sock the datagram socket.
     * @param profile the socket profile.
     * @param clock the source of wall clock time.
     */
    protected ReliableSocket(DatagramSocket sock, ReliableSocketProfile profile, Clock clock)
    {
        if (sock == null) {
            throw new NullPointerException("sock");
        }
        if (clock == null) {
            throw new NullPointerException("clock");
        }

        _clock = clock;
        init(sock, profile);
    }

    /**
     * Initializes socket and sets it up for receiving incoming traffic.
     *
     * @param sock    the datagram socket.
     * @param profile    the socket profile.
     */
    protected void init(DatagramSocket sock, ReliableSocketProfile profile)
    {
        _sock = sock;
        _profile = profile;
        _shutdownHook = new ShutdownHook();

        _sendBufferSize    = (_profile.maxSegmentSize() - Segment.RUDP_HEADER_LEN) * 32;
        _recvBufferSize = (_profile.maxSegmentSize() - Segment.RUDP_HEADER_LEN) * 32;

        _sendQueueSize = _profile.maxSendQueueSize();
        _recvQueueSize = _profile.maxRecvQueueSize();

        _rto = new RtoEstimator(_profile.retransmissionTimeout());
        _sendbuffer = new byte[_profile.maxSegmentSize()];

        /* Register shutdown hook */
        try {
            Runtime.getRuntime().addShutdownHook(_shutdownHook);
        }
        catch (IllegalStateException xcp) {
            if (DEBUG) {
                xcp.printStackTrace();
            }
        }

        _sockThread.start();
    }

    public void bind(SocketAddress bindpoint)
        throws IOException
    {
        _sock.bind(bindpoint);
    }

    public void connect(SocketAddress endpoint)
        throws IOException
    {
        connect(endpoint, 0);
    }

    public void connect(SocketAddress endpoint, int timeout)
        throws IOException
    {
        if (endpoint == null) {
            throw new IllegalArgumentException("connect: The address can't be null");
        }

        if (timeout < 0) {
            throw new IllegalArgumentException("connect: timeout can't be negative");
        }

        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (isConnected()) {
            throw new SocketException("already connected");
        }

        if (!(endpoint instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Unsupported address type");
        }

        _endpoint = (InetSocketAddress) endpoint;

        // Synchronize sequence numbers
        _state = SYN_SENT;
        Random rand = new Random(now());
        Segment syn = new SYNSegment(_counters.setSequenceNumber(rand.nextInt(MAX_SEQUENCE_NUMBER)),
                _profile.maxOutstandingSegs(),
                _profile.maxSegmentSize(),
                _profile.retransmissionTimeout(),
                _profile.cumulativeAckTimeout(),
                _profile.nullSegmentTimeout(),
                _profile.maxRetrans(),
                _profile.maxCumulativeAcks(),
                _profile.maxOutOfSequence(),
                _profile.maxAutoReset());

        sendAndQueueSegment(syn);

        // Wait for connection establishment (or timeout)
        boolean timedout = false;
        synchronized (this) {
            if (!isConnected()) {
                try {
                    if (timeout == 0) {
                        wait();
                    }
                    else {
                        long startTime = now();
                        wait(timeout);
                        if (now() - startTime >= timeout) {
                            timedout = true;
                        }
                    }
                }
                catch (InterruptedException xcp) {
                    xcp.printStackTrace();
                }
            }
        }

        if (_state == ESTABLISHED) {
            return;
        }

        synchronized (_unackedSentQueue) {
            _unackedSentQueue.clear();
            _unackedSentQueue.notifyAll();
        }

        _counters.reset();
        _retransmissionTimer.cancel();

        switch (_state) {
            case SYN_SENT:
                removeShutdownHook();
                connectionRefused();
                _state = CLOSED;
                if (timedout) {
                    throw new SocketTimeoutException();
                }
                throw new SocketException("Connection refused");
            case CLOSED:
            case CLOSE_WAIT:
                _state = CLOSED;
                throw new SocketException("Socket closed");
        }
    }

    public SocketChannel getChannel()
    {
        return null;
    }

    public InetAddress getInetAddress()
    {
        if (!isConnected()) {
            return null;
        }

        return ((InetSocketAddress)_endpoint).getAddress();
    }

    public int getPort()
    {
        if (!isConnected()) {
            return 0;
        }

        return ((InetSocketAddress)_endpoint).getPort();

    }

    public SocketAddress getRemoteSocketAddress()
    {
        if (!isConnected()) {
            return null;
        }

        return new InetSocketAddress(getInetAddress(), getPort());
    }

    public InetAddress getLocalAddress()
    {
        return _sock.getLocalAddress();
    }

    public int getLocalPort()
    {
        return _sock.getLocalPort();
    }

    public SocketAddress getLocalSocketAddress()
    {
        return _sock.getLocalSocketAddress();
    }

    public InputStream getInputStream()
        throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (!isConnected()) {
            throw new SocketException("Socket is not connected");
        }

        if (isInputShutdown()) {
            throw new SocketException("Socket input is shutdown");
        }

        return _in;
    }

    public OutputStream getOutputStream()
        throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (!isConnected()) {
            throw new SocketException("Socket is not connected");
        }

        if (isOutputShutdown()) {
            throw new SocketException("Socket output is shutdown");
        }

        return _out;
    }

    public synchronized void close()
        throws IOException
    {
        synchronized (_closeLock) {

            if (isClosed()) {
                return;
            }

            removeShutdownHook();

            switch (_state) {
                case SYN_SENT:
                    synchronized (this) {
                        notify();
                    }
                    break;
                case CLOSE_WAIT:
                case SYN_RCVD:
                case ESTABLISHED:
                    sendSegment(new FINSegment(_counters.nextSequenceNumber()));
                    closeImpl();
                    break;
                case CLOSED:
                    destroyTimers();
                    closeSocket();
                    break;
            }

            _closed = true;
            _state = CLOSED;

            synchronized (_unackedSentQueue) {
                _unackedSentQueue.notify();
            }

            synchronized (_recvQueueLock) {
                _recvQueueLock.notify();
            }
        }
    }

    public boolean isBound()
    {
        return _sock.isBound();
    }

    public boolean isConnected()
    {
        return _connected;
    }

    public boolean isClosed()
    {
        return _closed;
    }

    public void setSoTimeout(int timeout)
        throws SocketException
    {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout < 0");
        }

        _timeout = timeout;
    }

    public synchronized void setSendBufferSize(int size)
        throws SocketException
    {
        if (!(size > 0)) {
            throw new IllegalArgumentException("negative receive size");
        }

        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (isConnected()) {
            return;
        }

        _sendBufferSize = size;
    }

    public synchronized int getSendBufferSize()
        throws SocketException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        return _sendBufferSize;
    }

    public synchronized void setReceiveBufferSize(int size)
        throws SocketException
    {
        if (!(size > 0)) {
            throw new IllegalArgumentException("negative send size");
        }

        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (isConnected()) {
            return;
        }

        _recvBufferSize = size;
    }

    public synchronized int getReceiveBufferSize()
        throws SocketException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        return _recvBufferSize;
    }

    public void setTcpNoDelay(boolean on)
        throws SocketException
    {
        throw new SocketException("Socket option not supported");
    }

    public boolean getTcpNoDelay()
    {
        return false;
    }

    public synchronized void setKeepAlive(boolean on)
        throws SocketException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (!(_keepAlive ^ on)) {
            return;
        }

        _keepAlive = on;

        if (isConnected()) {
            if (_keepAlive) {
                _keepAliveTimer.schedule(_profile.nullSegmentTimeout() * 6,
                                         _profile.nullSegmentTimeout() * 6);
            }
            else {
                _keepAliveTimer.cancel();
            }
        }
    }

    public synchronized boolean getKeepAlive()
        throws SocketException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        return _keepAlive;
    }

    public void shutdownInput()
        throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (!isConnected()) {
            throw new SocketException("Socket is not connected");
        }

        if (isInputShutdown()) {
            throw new SocketException("Socket input is already shutdown");
        }

        _shutIn = true;

        synchronized (_recvQueueLock) {
            _recvQueueLock.notify();
        }
    }

    public void shutdownOutput()
        throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (!isConnected()) {
            throw new SocketException("Socket is not connected");
        }

        if (isOutputShutdown()) {
            throw new SocketException("Socket output is already shutdown");
        }

        _shutOut = true;

        synchronized (_unackedSentQueue) {
            _unackedSentQueue.notifyAll();
        }
    }

    public boolean isInputShutdown()
    {
        return _shutIn;
    }

    public boolean isOutputShutdown()
    {
        return _shutOut;
    }

    /**
     * Resets the socket state.
     * <p>
     * The socket will attempt to deliver all outstanding bytes to the remote
     * endpoint and then it will renegotiate the connection parameters.
     * The transmissions of bytes resumes after the renegotation finishes and
     * the connection is synchronized again.
     *
     * @throws IOException if an I/O error occurs when resetting the connection.
     */
    public void reset()
        throws IOException
    {
        reset(null);
    }

    /**
     * Resets the socket state and profile.
     * <p>
     * The socket will attempt to deliver all outstanding bytes to the remote
     * endpoint and then it will renegotiate the connection parameters
     * specified in the given socket profile.
     * The transmissions of bytes resumes after the renegotation finishes and
     * the connection is synchronized again.
     *
     * @param profile the socket profile or null if old profile should be used.
     *
     * @throws IOException if an I/O error occurs when resetting the connection.
     */
    public void reset(ReliableSocketProfile profile)
        throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (!isConnected()) {
            throw new SocketException("Socket is not connected");
        }

        synchronized (_resetLock) {
            _reset = true;

            sendAndQueueSegment(new RSTSegment(_counters.nextSequenceNumber()));

            // Wait to flush all outstanding segments (including last RST segment).
            synchronized (_unackedSentQueue) {
                while (!_unackedSentQueue.isEmpty()) {
                    try {
                        _unackedSentQueue.wait();
                    }
                    catch (InterruptedException xcp) {
                        xcp.printStackTrace();
                    }
                }
            }
        }

        connectionReset();

        // Set new profile
        if (profile != null) {
            _profile = profile;
        }

        // Synchronize sequence numbers
        _state = SYN_SENT;
        Random rand = new Random(now());
        Segment syn = new SYNSegment(_counters.setSequenceNumber(rand.nextInt(MAX_SEQUENCE_NUMBER)),
                _profile.maxOutstandingSegs(),
                _profile.maxSegmentSize(),
                _profile.retransmissionTimeout(),
                _profile.cumulativeAckTimeout(),
                _profile.nullSegmentTimeout(),
                _profile.maxRetrans(),
                _profile.maxCumulativeAcks(),
                _profile.maxOutOfSequence(),
                _profile.maxAutoReset());

        sendAndQueueSegment(syn);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> as data segments and
     * queues them for immediate transmission.
     *
     * @param  b      the data.
     * @param  off    the start offset in the data.
     * @param  len    the number of bytes to write.
     * @throws IOException if an I/O error occurs. In particular,
     *         an <code>IOException</code> is thrown if the socket
     *         is closed.
     */
    protected void write(byte[] b, int off, int len)
        throws IOException
    {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }

        if (isOutputShutdown()) {
            throw new IOException("Socket output is shutdown");
        }

        if (!isConnected()) {
            throw new SocketException("Connection reset");
        }

        int totalBytes = 0;
        while (totalBytes < len) {
            synchronized (_resetLock) {
                while (_reset) {
                    try {
                        _resetLock.wait();
                    }
                    catch (InterruptedException xcp) {
                        xcp.printStackTrace();
                    }
                }

                int writeBytes = Math.min(_profile.maxSegmentSize() - Segment.RUDP_HEADER_LEN,
                                         len - totalBytes);

                sendAndQueueSegment(new DATSegment(_counters.nextSequenceNumber(),
                        _counters.getLastInSequence(), b, off + totalBytes, writeBytes));
                totalBytes += writeBytes;
            }
        }
    }

    /**
     * Reads up to <code>len</code> bytes of data from the receiver
     * buffer into an array of bytes.  An attempt is made to read
     * as many as <code>len</code> bytes, but a smaller number may
     * be read. The number of bytes actually read is returned as
     * an integer.
     * <p>
     * This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * @param  b    the buffer into which the data is read.
     * @param  off  the start offset in array <code>b</code>
     *              at which the data is written.
     * @param  len  the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer,
     *         or <code>-1</code> if there is no more data because
     *         the end of the stream has been reached.
     * @throws IOException if an I/O error occurs. In particular,
     *         an <code>IOException</code> is thrown if the socket
     *         is closed, or if the buffer is not big enough to hold
     *         a full data segment.
     */
	protected int read(byte[] b, int off, int len)
        throws IOException
    {

        int totalBytes = 0;

        synchronized (_recvQueueLock) {

            while (true) {
                while (_inSeqRecvQueue.isEmpty()) {

                    if (isClosed()) {
                        throw new SocketException("Socket is closed");
                    }

                    if (isInputShutdown()) {
                        throw new EOFException();
                    }

                    if (!isConnected()) {
                        throw new SocketException("Connection reset");
                    }

                    try {
                        if (_timeout == 0) {
                            _recvQueueLock.wait();
                        }
                        else {
                            long startTime = now();
                            _recvQueueLock.wait(_timeout);
                            if ((now() - startTime) >= _timeout) {
                                throw new SocketTimeoutException();
                            }
                        }
                    }
                    catch (InterruptedException xcp) {
                    	if(!_closed)
                    		throw new InterruptedIOException(xcp.getMessage());
                    }
                }

                /*
                 * Drains the head of the in-sequence queue. Every element is
                 * either a data, reset or FIN segment (handleSegment only
                 * ever enqueues those), so peeking from the front and polling
                 * avoids the O(n) shifting an ArrayList removal would cost on
                 * every single segment.
                 */
                while (!_inSeqRecvQueue.isEmpty()) {
                    Segment s = _inSeqRecvQueue.peekFirst();

                    if (s instanceof RSTSegment) {
                        _inSeqRecvQueue.pollFirst();
                        break;
                    }
                    else if (s instanceof FINSegment) {
                        if (totalBytes <= 0) {
                            _inSeqRecvQueue.pollFirst();
                            return -1; /* EOF */
                        }
                        break;
                    }
                    else if (s instanceof DATSegment) {
                        byte[] data = ((DATSegment) s).getData();
                        if (data.length + totalBytes > len) {
                            if (totalBytes <= 0) {
                                throw new IOException("insufficient buffer space");
                            }
                            break;
                        }

                        System.arraycopy(data, 0, b, off+totalBytes, data.length);
                        totalBytes += data.length;
                        _inSeqRecvQueue.pollFirst();
                    }
                    else {
                        _inSeqRecvQueue.pollFirst();
                    }
                }

                if (totalBytes > 0) {
                    return totalBytes;
                }
            }
        }
    }

    /**
     * Adds the specified listener to this socket. If the listener
     * has already been registered, this method does nothing.
     *
     * @param listener the listener to add.
     */
    public void addListener(ReliableSocketListener listener)
    {
        if (listener == null) {
            throw new NullPointerException("listener");
        }

        synchronized (_listeners) {
            if (!_listeners.contains(listener)) {
                _listeners.add(listener);
            }
        }
    }

    /**
     * Removes the specified listener from this socket. This is
     * harmless if the listener was not previously registered.
     *
     * @param listener the listener to remove.
     */
    public void removeListener(ReliableSocketListener listener)
    {
        if (listener == null) {
            throw new NullPointerException("listener");
        }

        synchronized (_listeners) {
            _listeners.remove(listener);
        }
    }

    /**
     * Adds the specified state listener to this socket. If the listener
     * has already been registered, this method does nothing.
     *
     * @param stateListener the listener to add.
     */
    public void addStateListener(ReliableSocketStateListener stateListener)
    {
        if (stateListener == null) {
            throw new NullPointerException("stateListener");
        }

        synchronized (_stateListeners) {
            if (!_stateListeners.contains(stateListener)) {
                _stateListeners.add(stateListener);
            }
        }
    }

    /**
     * Removes the specified state listener from this socket. This is
     * harmless if the listener was not previously registered.
     *
     * @param stateListener the listener to remove.
     */
    public void removeStateListener(ReliableSocketStateListener stateListener)
    {
         if (stateListener == null) {
             throw new NullPointerException("stateListener");
         }

         synchronized (_stateListeners) {
            _stateListeners.remove(stateListener);
         }
    }

    /**
     * Returns the retransmission timeout to stamp on a segment that is being
     * (re)transmitted right now: the current RTO estimate scaled by the
     * segment's accumulated exponential backoff.
     * <p>
     * The backoff used to be computed and then discarded, while the deadline
     * was always stamped with the bare RTO, so a segment that had already
     * timed out was retried at the very same interval as the first attempt
     * and the retries piled onto an already congested path. The lower bound
     * needs no clamp: updateRttSample() already keeps the RTO at or above
     * MIN_RTO and the shift is never negative.
     *
     * @param segment the segment about to go out.
     * @return the effective timeout in milliseconds.
     */
    private int rtoFor(Segment segment)
    {
        return _rto.rtoFor(segment.rtoShift());
    }

    /**
     * Sends a segment piggy-backing any pending acknowledgments.
     *
     * @param  s the segment.
     * @throws IOException if an I/O error occurs in the
     *         underlying UDP socket.
     */
    private void sendSegment(Segment s)
        throws IOException
    {
        /* Piggyback any pending acknowledgments */
        if (s instanceof DATSegment || s instanceof RSTSegment || s instanceof FINSegment || s instanceof NULSegment) {
            checkAndSetAck(s);
        }

        /* Reset null segment timer */
        if (s instanceof DATSegment || s instanceof RSTSegment || s instanceof FINSegment) {
            _nullSegmentTimer.reset();
        }

        if (DEBUG) {
            log("sent " + s);
        }

        s.markSent(now(), rtoFor(s));
        sendSegmentImpl(s);
    }

    /**
     * Receives a segment and increases the cumulative
     * acknowledgment counter.
     *
     * @return the received segment.
     * @throws IOException if an I/O error occurs in the
     *         underlying UDP socket.
     */
    private Segment receiveSegment()
        throws IOException
    {
        Segment s;
        if ((s = receiveSegmentImpl()) != null) {

            if (DEBUG) {
                log("recv " + s);
            }

            if (s instanceof DATSegment || s instanceof NULSegment ||
                s instanceof RSTSegment || s instanceof FINSegment ||
                s instanceof SYNSegment) {
                _counters.incCumulativeAckCounter();
            }

            if (_keepAlive && !(s instanceof RSTSegment)) {
                _keepAliveTimer.reset();
            }
        }

        return s;
    }

    /**
     * Sends a segment and queues a copy of it in the queue of unacknowledged segments.
     *
     * @param  segment     a segment for which delivery must be guaranteed.
     * @throws IOException if an I/O error occurs in the
     *         underlying UDP socket.
     */
    private void sendAndQueueSegment(Segment segment)
        throws IOException
    {
        synchronized (_unackedSentQueue) {
            while ((_unackedSentQueue.size() >= _sendQueueSize) ||
                   (_counters.getOutstandingSegsCounter() > _profile.maxOutstandingSegs())) {
                if (!_connected) {
                    throw new SocketException("Socket is closed");
                }
                try {
                    _unackedSentQueue.wait();
                }
                catch (InterruptedException xcp) {
                    xcp.printStackTrace();
                }
            }

            _counters.incOutstandingSegsCounter();
            _unackedSentQueue.add(segment);

            /*
             * Stamp the deadline here, before arming, and not leave it to
             * sendSegment() below: armRetransmission() skips segments whose
             * deadline is still Long.MAX_VALUE, so arming an unstamped
             * segment silently dropped it from the retransmission schedule
             * and its first transmission was never retried. That is why a
             * lost SYN used to hang in SYN_SENT until connect()'s own
             * timeout instead of being retransmitted.
             */
            segment.markSent(now(), rtoFor(segment));
            armRetransmission(segment);
        }

        if (_closed) {
            throw new SocketException("Socket is closed");
        }

        sendSegment(segment);

        if (segment instanceof DATSegment) {
            synchronized (_listeners) {
                Iterator<ReliableSocketListener> it = _listeners.iterator();
                while (it.hasNext()) {
                    ReliableSocketListener l = (ReliableSocketListener) it.next();
                    l.packetSent();
                }
            }
        }
    }

    /**
     * Sends a segment and increments its retransmission counter.
     *
     * @param  segment    the segment to be retransmitted.
     * @return true if the segment exceeded the retransmission limit
     *         (the caller must call connectionFailure() once it has
     *         released the _unackedSentQueue monitor).
     * @throws IOException if an I/O error occurs in the
     *         underlying UDP socket.
     */
    private boolean retransmitSegment(Segment segment)
        throws IOException
    {
        if (_profile.maxRetrans() > 0) {
            segment.setRetxCounter(segment.getRetxCounter()+1);
        }

        if (_profile.maxRetrans() != 0 && segment.getRetxCounter() > _profile.maxRetrans()) {
            return true;
        }

        segment.markRetransmitted();
        sendSegment(segment);

        synchronized (_unackedSentQueue) {
            armRetransmission(segment);
        }

        if (segment instanceof DATSegment) {
             synchronized (_listeners) {
                 Iterator<ReliableSocketListener> it = _listeners.iterator();
                 while (it.hasNext()) {
                     ReliableSocketListener l = (ReliableSocketListener) it.next();
                     l.packetRetransmitted();
                 }
             }
        }

        return false;
    }

    /**
     * Puts a segment into the retransmission schedule, ordered by its
     * deadline, and makes sure the retransmission timer is armed for the
     * earliest one.
     * <p>
     * Must be called while holding the _unackedSentQueue monitor.
     * Re-arming an already scheduled segment is a no-op: the heap entry is
     * replaced only when the deadline actually moved, so that a burst of
     * retransmissions does not grow the heap.
     */
    private void armRetransmission(Segment segment)
    {
        _retxQueue.remove(segment);
        if (segment.deadline() != Long.MAX_VALUE) {
            _retxQueue.add(segment);
        }

        Segment head = _retxQueue.peek();
        if (head == null) {
            _retransmissionTimer.cancel();
            return;
        }

        long delay = head.deadline() - now();
        if (delay < 1) {
            delay = 1;
        }

        if (_retransmissionTimer.isIdle()) {
            _retransmissionTimer.schedule(delay);
        }
    }

    /**
     * Re-arms the retransmission timer for the earliest deadline still
     * pending. Must be called while holding the _unackedSentQueue monitor.
     */
    private void rearmRetransmission()
    {
        Segment head = _retxQueue.peek();
        if (head == null) {
            _retransmissionTimer.cancel();
            return;
        }

        long delay = head.deadline() - now();
        if (delay < 1) {
            delay = 1;
        }

        _retransmissionTimer.cancel();
        _retransmissionTimer.schedule(delay);
    }

    /**
     * Folds a round trip time sample into the retransmission timeout
     * estimator (RFC 6298). Must be called while holding the
     * _unackedSentQueue monitor.
     */
    private void updateRttSample(long rttMillis)
    {
        _rto.updateSample(rttMillis);
    }

    /**
     * Puts the connection in an "opened" state and notifies all
     * registered state listeners that the connection is opened.
     */
    private void connectionOpened()
    {
        if (isConnected()) {

            _nullSegmentTimer.cancel();

            if (_keepAlive) {
                _keepAliveTimer.cancel();
            }

            synchronized (_resetLock) {
                _reset = false;
                _resetLock.notify();
            }
        }
        else {
            synchronized (this) {
                try {
                    _in = new ReliableSocketInputStream(this);
                    _out = new ReliableSocketOutputStream(this);
                    _connected = true;
                    _state = ESTABLISHED;
                }
                catch (IOException xcp) {
                    xcp.printStackTrace();
                }

                notify();
            }

            synchronized (_stateListeners) {
                Iterator<ReliableSocketStateListener> it = _stateListeners.iterator();
                while (it.hasNext()) {
                    ReliableSocketStateListener l = (ReliableSocketStateListener) it.next();
                    l.connectionOpened(this);
                }
            }
        }

        _nullSegmentTimer.schedule(0, _profile.nullSegmentTimeout());

        if (_keepAlive) {
            _keepAliveTimer.cancel();
            _keepAliveTimer.schedule(_profile.nullSegmentTimeout() * 6,
                                     _profile.nullSegmentTimeout() * 6);
        }
    }

    /**
     * Notifies all registered state listeners that
     * the connection attempt has been refused.
     */
    private void connectionRefused()
    {
        synchronized (_stateListeners) {
            Iterator<ReliableSocketStateListener> it = _stateListeners.iterator();
            while (it.hasNext()) {
                ReliableSocketStateListener l = (ReliableSocketStateListener) it.next();
                l.connectionRefused(this);
            }
        }
    }

    /**
     * Notifies all registered state listeners
     * that the connection has been closed.
     */
    private void connectionClosed()
    {
        synchronized (_stateListeners) {
            Iterator<ReliableSocketStateListener> it = _stateListeners.iterator();
            while (it.hasNext()) {
                ReliableSocketStateListener l = (ReliableSocketStateListener) it.next();
                l.connectionClosed(this);
            }
        }
    }

    /**
     * Puts the connection in a closed state and notifies all
     * registered state listeners that the connection failed.
     */
    private void connectionFailure()
    {
        synchronized (_closeLock) {

            if (isClosed()) {
                return;
            }

            removeShutdownHook();

            switch (_state) {
                case SYN_SENT:
                    synchronized (this) {
                        notify();
                    }
                    break;
                case CLOSE_WAIT:
                case SYN_RCVD:
                case ESTABLISHED:
                    _connected = false;
                    synchronized (_unackedSentQueue) {
                        _unackedSentQueue.notifyAll();
                    }

                    synchronized (_recvQueueLock) {
                        _recvQueueLock.notify();
                    }

                    closeImpl();
                    break;
            }

            _state = CLOSED;
            _closed = true;
        }

        synchronized (_stateListeners) {
            Iterator<ReliableSocketStateListener> it = _stateListeners.iterator();
            while (it.hasNext()) {
                ReliableSocketStateListener l = (ReliableSocketStateListener) it.next();
                l.connectionFailure(this);
            }
        }
    }

    /**
     * Notifies all registered state listeners
     * that the connection has been reset.
     */
    private void connectionReset()
    {
        synchronized (_stateListeners) {
            Iterator<ReliableSocketStateListener> it = _stateListeners.iterator();
            while (it.hasNext()) {
                ReliableSocketStateListener l = (ReliableSocketStateListener) it.next();
                l.connectionReset(this);
            }
        }
    }

    /**
     * Handles a received SYN segment.
     * <p>
     * When a client initiates a connection it sends a SYN segment which
     * contains the negotiable parameters defined by the Upper Layer Protocol
     * via the API. The server can accept these parameters by echoing them back
     * in its SYN with ACK response or propose different parameters in its SYN
     * with ACK response. The client can then choose to accept the parameters
     * sent by the server by sending an ACK to establish the connection or it can
     * refuse the connection by sending a FIN.
     *
     * @param segment the SYN segment.
     *
     */
    private void handleSYNSegment(SYNSegment segment)
    {
        try {
            switch (_state) {
                case CLOSED:
                    _counters.setLastInSequence(segment.seq());
                    _state = SYN_RCVD;

                    if (_keepAlive) {
                        _keepAliveTimer.schedule(SYN_RCVD_TIMEOUT);
                    }

                    Random rand = new Random(now());
                    _profile = new ReliableSocketProfile(
                            _sendQueueSize,
                            _recvQueueSize,
                            segment.getMaxSegmentSize(),
                            segment.getMaxOutstandingSegments(),
                            segment.getMaxRetransmissions(),
                            segment.getMaxCumulativeAcks(),
                            segment.getMaxOutOfSequence(),
                            segment.getMaxAutoReset(),
                            segment.getNulSegmentTimeout(),
                            segment.getRetransmissionTimeout(),
                            segment.getCummulativeAckTimeout());

                    Segment syn = new SYNSegment(_counters.setSequenceNumber(rand.nextInt(MAX_SEQUENCE_NUMBER)),
                            _profile.maxOutstandingSegs(),
                            _profile.maxSegmentSize(),
                            _profile.retransmissionTimeout(),
                            _profile.cumulativeAckTimeout(),
                            _profile.nullSegmentTimeout(),
                            _profile.maxRetrans(),
                            _profile.maxCumulativeAcks(),
                            _profile.maxOutOfSequence(),
                            _profile.maxAutoReset());

                    syn.setAck(segment.seq());
                    sendAndQueueSegment(syn);
                    break;
                case SYN_SENT:
                    _counters.setLastInSequence(segment.seq());
                    _state = ESTABLISHED;
                    /*
                     * Here the client accepts or rejects the parameters sent by the
                     * server. For now we will accept them.
                     */
                    sendAck();
                    connectionOpened();
                    break;
            }
        }
        catch (IOException xcp) {
            xcp.printStackTrace();
        }
    }

    /**
     * Handles a received EAK segment.
     * <p>
     * When a EAK segment is received, the segments specified in
     * the message are removed from the unacknowledged sent queue.
     * The segments to be retransmitted are determined by examining
     * the Ack Number and the last out of sequence ack number in the
     * EAK segment. All segments between but not including these two
     * sequence numbers that are on the unacknowledged sent queue are
     * retransmitted.
     *
     * @param segment the EAK segment.
     */
    private void handleEAKSegment(EAKSegment segment)
    {
        Iterator<Segment> it;
        int[] acks = segment.getACKs();

        int lastInSequence = segment.getAck();
        int lastOutSequence = acks[acks.length-1];
        boolean limitExceeded = false;

        synchronized (_unackedSentQueue) {

            /* Removed acknowledged segments from sent queue */
            int removed = 0;
            for (it = _unackedSentQueue.iterator(); it.hasNext(); ) {
                Segment s = (Segment) it.next();
                if ((compareSequenceNumbers(s.seq(), lastInSequence) <= 0)) {
                    it.remove();
                    s.markAcked();
                    s.clearBackOff();
                    removed++;
                    continue;
                }

                for (int i = 0; i < acks.length; i++) {
                    if ((compareSequenceNumbers(s.seq(), acks[i]) == 0)) {
                        it.remove();
                        s.markAcked();
                        s.clearBackOff();
                        removed++;
                        break;
                    }
                }
            }

            _counters.decOutstandingSegsCounter(removed);

            /* Retransmit segments */
            it = _unackedSentQueue.iterator();
            while (it.hasNext() && !limitExceeded) {
                Segment s = (Segment) it.next();
                if ((compareSequenceNumbers(lastInSequence, s.seq()) < 0) &&
                    (compareSequenceNumbers(lastOutSequence, s.seq()) > 0)) {

                    try {
                        limitExceeded = retransmitSegment(s);
                    }
                    catch (IOException xcp) {
                        xcp.printStackTrace();
                    }
                }
            }

            if (_unackedSentQueue.isEmpty()) {
                _retxQueue.clear();
                _retransmissionTimer.cancel();
            }
            else {
                rearmRetransmission();
            }

            _unackedSentQueue.notifyAll();
        }

        if (limitExceeded) {
            connectionFailure();
        }
    }

    /**
     * Handles a received RST, FIN, or DAT segment.
     *
     * @param segment
     */
    private void handleSegment(Segment segment)
    {
        /*
         * When a RST segment is received, the sender must stop
         * sending new packets, but most continue to attempt
         * delivery of packets already accepted from the application.
         */
        if (segment instanceof RSTSegment) {
            synchronized (_resetLock) {
                _reset = true;
            }

            connectionReset();
        }

        /*
         * When a FIN segment is received, no more packets
         * are expected to arrive after this segment.
         */
        if (segment instanceof FINSegment) {
            switch (_state) {
                case SYN_SENT:
                    synchronized (this) {
                        notify();
                    }
                    break;
                case CLOSED:
                    break;
                default:
                    _state = CLOSE_WAIT;
            }
        }

        boolean inSequence = false;
        synchronized (_recvQueueLock) {

            if (compareSequenceNumbers(segment.seq(), _counters.getLastInSequence()) <= 0) {
                /* Drop packet: duplicate. */
            }
            else if (compareSequenceNumbers(segment.seq(), nextSequenceNumber(_counters.getLastInSequence())) == 0) {
                inSequence = true;
                if (_inSeqRecvQueue.size() == 0 || (_inSeqRecvQueue.size() + _outSeqRecvQueue.size() < _recvQueueSize)) {
                    /* Insert in-sequence segment */
                    _counters.setLastInSequence(segment.seq());
                    if (segment instanceof DATSegment || segment instanceof RSTSegment || segment instanceof FINSegment) {
                        _inSeqRecvQueue.add(segment);
                    }

                    if (segment instanceof DATSegment) {
                        synchronized (_listeners) {
                            Iterator<ReliableSocketListener> it = _listeners.iterator();
                            while (it.hasNext()) {
                                ReliableSocketListener l = (ReliableSocketListener) it.next();
                                l.packetReceivedInOrder();
                            }
                        }
                    }

                    checkRecvQueues();
                }
                else {
                    /* Drop packet: queue is full. */
                }
            }
            else if (_inSeqRecvQueue.size() + _outSeqRecvQueue.size() < _recvQueueSize) {
                /* Insert out-of-sequence segment, in order */
                boolean added = false;
                for (int i = 0; i < _outSeqRecvQueue.size() && !added; i++) {
                    Segment s = (Segment) _outSeqRecvQueue.get(i);
                    int cmp = compareSequenceNumbers(segment.seq(), s.seq());
                    if (cmp == 0) {
                        /* Ignore duplicate packet */
                        added = true;
                    }
                    else if (cmp < 0) {
                        _outSeqRecvQueue.add(i, segment);
                        added = true;
                    }
                }

                if (!added) {
                    _outSeqRecvQueue.add(segment);
                }

                _counters.incOutOfSequenceCounter();

                if (segment instanceof DATSegment) {
                    synchronized (_listeners) {
                        Iterator<ReliableSocketListener> it = _listeners.iterator();
                        while (it.hasNext()) {
                            ReliableSocketListener l = (ReliableSocketListener) it.next();
                            l.packetReceivedOutOfOrder();
                        }
                    }
                }
            }

            if (inSequence && (segment instanceof RSTSegment ||
                               segment instanceof NULSegment ||
                               segment instanceof FINSegment)) {
                sendAck();
            }
            else if ((_counters.getOutOfSequenceCounter() > 0) &&
                (_profile.maxOutOfSequence() == 0 || _counters.getOutOfSequenceCounter() > _profile.maxOutOfSequence())) {
                sendExtendedAck();
            }
            else if ((_counters.getCumulativeAckCounter() > 0) &&
                     (_profile.maxCumulativeAcks() == 0 || _counters.getCumulativeAckCounter() > _profile.maxCumulativeAcks())) {
                sendSingleAck();
            }
            else {
                synchronized (_cumulativeAckTimer) {
                    if (_cumulativeAckTimer.isIdle()) {
                        _cumulativeAckTimer.schedule(_profile.cumulativeAckTimeout());
                    }
                }
            }
        }
    }

    /**
     * Acknowledges the next segment to be acknowledged.
     * If there are any out-of-sequence segments in the
     * receiver queue, it sends an EAK segment.
     */
    private void sendAck()
    {
        synchronized (_recvQueueLock) {
            if (!_outSeqRecvQueue.isEmpty()) {
                sendExtendedAck();
                return;
            }

            sendSingleAck();
        }
    }

    /**
     * Sends an EAK segment if there is at least one
     * out-of-sequence received segment.
     */
    private void sendExtendedAck()
    {
        synchronized (_recvQueueLock) {

            if (_outSeqRecvQueue.isEmpty()) {
                return;
            }

            _counters.getAndResetCumulativeAckCounter();
            _counters.getAndResetOutOfSequenceCounter();

            /* Compose list of out-of-sequence sequence numbers */
            int[] acks = new int[_outSeqRecvQueue.size()];
            for (int i = 0; i < acks.length; i++) {
                Segment s = (Segment) _outSeqRecvQueue.get(i);
                acks[i] = s.seq();
            }

            try {
                int lastInSequence = _counters.getLastInSequence();
                sendSegment(new EAKSegment(nextSequenceNumber(lastInSequence),
                        lastInSequence, acks));
            }
            catch (IOException xcp) {
                xcp.printStackTrace();
            }

        }
    }

    /**
     * Sends an ACK segment if there is a received segment to
     * be acknowledged.
     */
    private void sendSingleAck()
    {
        if (_counters.getAndResetCumulativeAckCounter() == 0) {
            return;
        }

        try {
            int lastInSequence = _counters.getLastInSequence();
            sendSegment(new ACKSegment(nextSequenceNumber(lastInSequence), lastInSequence));
        }
        catch (IOException xcp) {
            xcp.printStackTrace();
        }
    }

    /**
     * Sets the ACK flag and number of a segment if there is at least
     * one received segment to be acknowledged.
     *
     * @param s the segment.
     */
    private void checkAndSetAck(Segment s)
    {
        if (_counters.getAndResetCumulativeAckCounter() == 0) {
            return;
        }

        s.setAck(_counters.getLastInSequence());
    }

    /**
     * Checks the ACK flag and number of a segment.
     *
     * @param segment the segment.
     */
    private void checkAndGetAck(Segment segment)
    {
        int ackn = segment.getAck();

        if (ackn < 0) {
            return;
        }

        if (_state == SYN_RCVD) {
            _state = ESTABLISHED;
            connectionOpened();
        }

        boolean fastRetransmit = false;
        boolean limitExceeded = false;

        synchronized (_unackedSentQueue) {
            Segment newestAcked = null;
            int removed = 0;

            Iterator<Segment> it = _unackedSentQueue.iterator();
            while (it.hasNext()) {
                Segment s = (Segment) it.next();
                if (compareSequenceNumbers(s.seq(), ackn) <= 0) {
                    it.remove();
                    s.markAcked();
                    s.clearBackOff();
                    newestAcked = s;
                    removed++;
                }
            }

            _counters.decOutstandingSegsCounter(removed);

            /*
             * An ACK that does not advance the acknowledgment point is a
             * duplicate: three of them in a row mean the segment right after
             * the acknowledged one was most likely lost, so retransmit it
             * right away instead of waiting for its timeout to expire.
             */
            if (!_dupAckValid || compareSequenceNumbers(ackn, _lastAckn) != 0) {
                _lastAckn = ackn;
                _dupAckValid = true;
                _dupAcks = 0;
            }
            else if (++_dupAcks >= FAST_RETRANSMIT_THRESHOLD) {
                _dupAcks = 0;
                fastRetransmit = true;
            }

            if (newestAcked != null && !newestAcked.wasRetransmitted()) {
                updateRttSample(now() - newestAcked.sentTime());
            }

            if (fastRetransmit && !_unackedSentQueue.isEmpty()) {
                Segment lost = _unackedSentQueue.get(0);
                lost.backOffRto(FAST_RETRANSMIT_BACKOFF_SHIFT);
                try {
                    limitExceeded = retransmitSegment(lost);
                }
                catch (IOException xcp) {
                    xcp.printStackTrace();
                }
            }

            if (_unackedSentQueue.isEmpty()) {
                _retxQueue.clear();
                _retransmissionTimer.cancel();
            }
            else {
                rearmRetransmission();
            }

            _unackedSentQueue.notifyAll();
        }

        if (limitExceeded) {
            connectionFailure();
        }
    }

    /**
     * Checks for in-sequence segments in the out-of-sequence queue
     * that can be moved to the in-sequence queue.
     */
    private void checkRecvQueues()
    {
        synchronized (_recvQueueLock) {
            Iterator<Segment> it = _outSeqRecvQueue.iterator();
            while (it.hasNext()) {
                Segment s = (Segment) it.next();
                if (compareSequenceNumbers(s.seq(), nextSequenceNumber(_counters.getLastInSequence())) == 0) {
                    _counters.setLastInSequence(s.seq());
                    if (s instanceof DATSegment || s instanceof RSTSegment || s instanceof FINSegment) {
                        _inSeqRecvQueue.add(s);
                    }
                    it.remove();
                }
            }

            _recvQueueLock.notify();
        }
    }

    /**
     * Writes out a segment to the underlying UDP socket.
     *
     * @param  s    the segment.
     * @throws IOException if an I/O error occurs in the
     *         underlying UDP socket.
     */
    protected void sendSegmentImpl(Segment s)
        throws IOException
    {
        try {
            /*
             * The serialization buffer and the packet are reused across
             * calls. Sends are serialized because this method is reached
             * concurrently from the application thread, the socket reader
             * thread and the timer threads.
             */
            synchronized (_sendLock) {
                int len = s.length();
                if (_sendbuffer.length < len) {
                    _sendbuffer = new byte[len];
                    _sendPacket = null;
                }

                if (_sendPacket == null) {
                    _sendPacket = new DatagramPacket(_sendbuffer, len, _endpoint);
                }
                else {
                    _sendPacket.setData(_sendbuffer);
                    _sendPacket.setLength(len);
                    _sendPacket.setSocketAddress(_endpoint);
                }

                s.writeTo(_sendbuffer, 0);
                _sock.send(_sendPacket);
            }
        }
        catch (IOException xcp) {
        }
    }

    /**
     * Reads in a segment from the underlying UDP socket.
     *
     * @return s    the segment.
     * @throws IOException if an I/O error occurs in the
     *         underlying UDP socket.
     */
    protected Segment receiveSegmentImpl()
        throws IOException
    {
        try {
            if (_recvPacket == null) {
                _recvPacket = new DatagramPacket(_recvbuffer, _recvbuffer.length);
            }

            _sock.receive(_recvPacket);
            return Segment.parse(_recvbuffer, 0, _recvPacket.getLength());
        }
        catch (IOException ioXcp) {
            if (!isClosed()) {
                ioXcp.printStackTrace();
            }
        }

        return null;
    }

    /**
     * Destroys all four internal timer threads (retransmission, cumulative-ack,
     * keep-alive, null-segment). Each is a Thread that was started as soon as
     * this object was constructed (see field initializers below) and stays
     * parked in Timer.run()'s wait() - scheduled or not - until destroy() is
     * called on it. Exposed as protected so that subclasses can guarantee
     * these threads terminate even when releasing a connection outside the
     * normal close()/closeImpl() flow (e.g. cleaning up after a failed
     * connect() attempt, which never calls close() itself).
     *
     * Safe to call more than once or on timers that were never scheduled;
     * Timer.destroy() just flips a couple of flags and notifies.
     */
    protected void destroyTimers()
    {
        _retransmissionTimer.destroy();
        _cumulativeAckTimer.destroy();
        _keepAliveTimer.destroy();
        _nullSegmentTimer.destroy();
    }

    /**
     * Unregisters this socket's shutdown hook from the JVM, if it is
     * still registered.
     */
    protected void removeShutdownHook()
    {
        try {
            Runtime.getRuntime().removeShutdownHook(_shutdownHook);
        }
        catch (IllegalStateException xcp) {
            if (DEBUG) {
                xcp.printStackTrace();
            }
        }
    }

    /**
     * Closes the underlying UDP socket.
     */
    protected void closeSocket()
    {
        _sock.close();
    }

    /**
     * Cleans up and closes the socket.
     */
    protected void closeImpl()
    {
        _nullSegmentTimer.cancel();
        _keepAliveTimer.cancel();
        _state = CLOSE_WAIT;

        Thread t = new Thread() {
            public void run()
            {
                _keepAliveTimer.destroy();
                _nullSegmentTimer.destroy();

                try {
                    Thread.sleep(_profile.nullSegmentTimeout() * 2);
                }
                catch (InterruptedException xcp) {
                    xcp.printStackTrace();
                }

                _retransmissionTimer.destroy();
                _cumulativeAckTimer.destroy();

                closeSocket();
                connectionClosed();
            }
        };
        t.setName("ReliableSocket-Closing");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Log routine.
     */
    protected void log(String msg)
    {
        System.out.println(getLocalPort() + ": " + msg);
    }

    /**
     * Returns this socket's current time, i.e. the injected clock's reading.
     *
     * @return the current time in milliseconds.
     */
    protected long now()
    {
        return _clock.currentTimeMillis();
    }

    /**
     * Computes the consecutive sequence number.
     *
     * @return the next number in the sequence.
     */
    private static int nextSequenceNumber(int seqn)
    {
        return (seqn + 1) % MAX_SEQUENCE_NUMBER;
    }

    /**
     * Compares two sequence numbers.
     *
     * @return 0, 1 or -1 if the first sequence number is equal,
     *         greater or less than the second sequence number.
     *         (see RFC 1982).
     */
    private int compareSequenceNumbers(int seqn, int aseqn)
    {
        if (seqn == aseqn) {
            return 0;
        }
        else if (((seqn < aseqn) && ((aseqn - seqn) > MAX_SEQUENCE_NUMBER/2)) ||
                 ((seqn > aseqn) && ((seqn - aseqn) < MAX_SEQUENCE_NUMBER/2))) {
            return 1;
        }
        else {
            return -1;
        }
    }

    protected DatagramSocket       _sock;
    protected SocketAddress        _endpoint;
    protected ReliableSocketInputStream  _in;
    protected ReliableSocketOutputStream _out;

    /*
     * Every wall clock reading of this socket goes through _clock, so a test
     * can install a clock it advances by hand and drive the retransmission
     * schedule deterministically instead of sleeping on the real one.
     */
    private Clock _clock = Clock.SYSTEM;

    private byte[]  _recvbuffer = new byte[65535];
    private DatagramPacket _recvPacket;

    /*
     * Outgoing packets are serialized into a single reusable buffer. The
     * buffer is reallocated when a segment larger than the negotiated
     * maximum shows up (a peer proposing a bigger maxSegmentSize).
     */
    private byte[] _sendbuffer = new byte[0];
    private DatagramPacket _sendPacket;
    private final Object _sendLock = new Object();

    private volatile boolean _closed = false;
    private boolean _connected = false;
    private boolean _reset     = false;
    private boolean _keepAlive = true;
    private int     _state     = CLOSED;
    private int     _timeout   = 0; /* (ms) */
    private boolean _shutIn  = false;
    private boolean _shutOut = false;

    private Object  _closeLock = new Object();
    private Object  _resetLock = new Object();

    private ArrayList<ReliableSocketListener> _listeners = new ArrayList<ReliableSocketListener>();
    private ArrayList<ReliableSocketStateListener> _stateListeners = new ArrayList<ReliableSocketStateListener>();

    private ShutdownHook _shutdownHook;

    /* RUDP connection parameters */
    private ReliableSocketProfile _profile = new ReliableSocketProfile();

    private ArrayList<Segment> _unackedSentQueue = new ArrayList<Segment>(); /* Unacknowledged segments send queue */
    private ArrayList<Segment> _outSeqRecvQueue  = new ArrayList<Segment>(); /* Out-of-sequence received segments queue */
    private ArrayDeque<Segment> _inSeqRecvQueue  = new ArrayDeque<Segment>(); /* In-sequence received segments queue */

    /*
     * Retransmission schedule: the unacknowledged segments ordered by the
     * instant at which their timeout retransmission becomes due. Guarded by
     * the _unackedSentQueue monitor.
     */
    private final PriorityQueue<Segment> _retxQueue = new PriorityQueue<Segment>(16,
            new Comparator<Segment>() {
                public int compare(Segment a, Segment b) {
                    return Long.compare(a.deadline(), b.deadline());
                }
            });

    /*
     * Round trip time estimation and the retransmission timeout derived from
     * it (RFC 6298). A fresh estimator has taken no sample yet and reports
     * the profile's retransmission timeout.
     * Guarded by the _unackedSentQueue monitor.
     */
    private RtoEstimator _rto;

    private int _dupAcks = 0;
    private int _lastAckn = -1;
    private boolean _dupAckValid = false;

    private Object _recvQueueLock = new Object();  /* Lock for receiver queues */
    private Counters _counters    = new Counters(); /* Sequence number, ack counters, etc. */

    private Thread _sockThread    = new ReliableSocketThread();

    private int _sendQueueSize = ReliableSocketProfile.MAX_SEND_QUEUE_SIZE;
    private int _recvQueueSize = ReliableSocketProfile.MAX_RECV_QUEUE_SIZE;

    private int _sendBufferSize;
    private int _recvBufferSize;

    /*
     * This timer is started when the connection is opened and is reset
     * every time a data segment is sent. If the client's null segment
     * timer expires, the client sends a null segment to the server.
     */
    private Timer _nullSegmentTimer =
        new Timer("ReliableSocket-NullSegmentTimer", new NullSegmentTimerTask());

    /*
     * This timer holds the retransmission schedule (_retxQueue). It is armed
     * as a one-shot for the earliest pending deadline and re-armed by
     * RetransmissionTimerTask itself, so each unacknowledged segment is
     * retransmitted on its own exponentially backed-off timeout rather than
     * every unacknowledged segment being resent on a single fixed tick.
     */
    private Timer _retransmissionTimer =
        new Timer("ReliableSocket-RetransmissionTimer", new RetransmissionTimerTask());

    /*
     * When this timer expires, if there are segments on the out-of-sequence
     * queue, an extended acknowledgment is sent. Otherwise, if there are
     * any segments currently unacknowledged, a stand-alone acknowledgment
     * is sent.
     * The cumulative acknowledge timer is restarted whenever an acknowledgment
     * is sent in a data, null, or reset segment, provided that there are no
     * segments currently on the out-of-sequence queue. If there are segments
     * on the out-of-sequence queue, the timer is not restarted, so that another
     * extended acknowledgment will be sent when it expires again.
     */
    private Timer _cumulativeAckTimer =
        new Timer("ReliableSocket-CumulativeAckTimer", new CumulativeAckTimerTask());

    /*
     * When this timer expires, the connection is considered broken.
     */
    private Timer _keepAliveTimer =
        new Timer("ReliableSocket-KeepAliveTimer", new KeepAliveTimerTask());

    /*
     * Hard cap on how long an incoming connection may sit in SYN_RCVD
     * waiting for the handshake-completing ACK. Deliberately NOT derived
     * from _profile or from the peer's SYN segment (both of which may
     * carry maxRetrans()==0, i.e. "unlimited retransmissions") - a remote
     * peer must not be able to extend or disable this timeout. If the ACK
     * never arrives, the incoming candidate socket (and its shutdown
     * hook, timers, buffers) would otherwise leak forever, since
     * _keepAliveTimer is not scheduled until ESTABLISHED is reached.
     */
    private static final long SYN_RCVD_TIMEOUT = 30000;

    /*
     * Number of consecutive acknowledgments that do not advance the
     * acknowledgment point before the oldest unacknowledged segment is
     * retransmitted without waiting for its timeout.
     */
    private static final int FAST_RETRANSMIT_THRESHOLD = 3;

    /*
     * A fast retransmission happens without any evidence of congestion, so
     * its timeout is not backed off at all (RtoEstimator.MAX_BACKOFF_SHIFT
     * instead).
     */
    private static final int FAST_RETRANSMIT_BACKOFF_SHIFT = 0;

    private static final int MAX_SEQUENCE_NUMBER        = 255;

    private static final int CLOSED      = 0; /* There is not an active or pending connection */
    private static final int SYN_RCVD    = 1; /* Request to connect received, waiting ACK */
    private static final int SYN_SENT    = 2; /* Request to connect sent */
    private static final int ESTABLISHED = 3; /* Data transfer state */
    private static final int CLOSE_WAIT  = 4; /* Request to close the connection */

    private static final boolean DEBUG = Boolean.getBoolean("net.rudp.debug");

    /*
     * -----------------------------------------------------------------------
     * INTERNAL CLASSES
     * -----------------------------------------------------------------------
     */

    private class Counters
    {
        public Counters()
        {
        }

        public synchronized int nextSequenceNumber()
        {
            return (_seqn = ReliableSocket.nextSequenceNumber(_seqn));
        }

        public synchronized int setSequenceNumber(int n)
        {
            _seqn = n;
            return _seqn;
        }

        /*
         * _lastInSequence is written only from the socket reader thread
         * (handleSYNSegment/handleSegment/checkRecvQueues), so a volatile
         * read is enough and keeps the per-segment hot path lock free.
         */
        public int setLastInSequence(int n)
        {
            _lastInSequence = n;
            return _lastInSequence;
        }

        public int getLastInSequence()
        {
            return _lastInSequence;
        }

        public void incCumulativeAckCounter()
        {
            _cumAckCounter.incrementAndGet();
        }

        public int getCumulativeAckCounter()
        {
            return _cumAckCounter.get();
        }

        public int getAndResetCumulativeAckCounter()
        {
            return _cumAckCounter.getAndSet(0);
        }

        public void incOutOfSequenceCounter()
        {
            _outOfSeqCounter.incrementAndGet();
        }

        public int getOutOfSequenceCounter()
        {
            return _outOfSeqCounter.get();
        }

        public int getAndResetOutOfSequenceCounter()
        {
            return _outOfSeqCounter.getAndSet(0);
        }

        public void incOutstandingSegsCounter()
        {
            _outSegsCounter.incrementAndGet();
        }

        public int getOutstandingSegsCounter()
        {
            return _outSegsCounter.get();
        }

        /**
         * Accounts for the segments that just left the unacknowledged queue.
         * Resetting the counter instead would silently widen the flow control
         * window by however many segments were still in flight.
         */
        public void decOutstandingSegsCounter(int n)
        {
            if (n > 0) {
                _outSegsCounter.addAndGet(-n);
            }
        }

        public void reset()
        {
            _outOfSeqCounter.set(0);
            _outSegsCounter.set(0);
            _cumAckCounter.set(0);
        }

        private int _seqn;             /* Segment sequence number */
        private volatile int _lastInSequence;   /* Last in-sequence received segment */

        /*
         * The receiver maintains a counter of unacknowledged segments received
         * without an acknowledgment being sent to the transmitter. The maximum
         * value of this counter is configurable. If this counter's maximum is
         * exceeded, the receiver sends either a stand-alone acknowledgment, or
         * an extended acknowledgment if there are currently any out-of-sequence
         * segments. The recommended value for the cumulative acknowledge counter
         * is 3.
         */
        private final AtomicInteger _cumAckCounter = new AtomicInteger(); /* Cumulative acknowledge counter */

        /*
         * The receiver maintains a counter of the number of segments that have
         * arrived out-of-sequence. Each time this counter exceeds its configurable
         * maximum, an extended acknowledgment segment containing the sequence
         * numbers of all current out-of-sequence segments that have been received
         * is sent to the transmitter. The counter is then reset to zero. The
         * recommended value for the out-of-sequence acknowledgments counter is 3.
         */
        private final AtomicInteger _outOfSeqCounter = new AtomicInteger(); /* Out-of-sequence acknowledgments counter */

        /*
         * The transmitter maintains a counter of the number of segments that
         * have been sent without getting an acknowledgment. This is used
         * by the receiver as a mean of flow control.
         */
        private final AtomicInteger _outSegsCounter = new AtomicInteger(); /* Outstanding segments counter */
    }

    private class ReliableSocketThread extends Thread
    {
        public ReliableSocketThread()
        {
            super("ReliableSocket");
            setDaemon(true);
        }

        public void run()
        {
            try {
                while (true) {
                    try {
                        Segment segment = receiveSegment();

                        if (segment == null) {
                            break;
                        }

                        if (segment instanceof SYNSegment) {
                            handleSYNSegment((SYNSegment) segment);
                        }
                        else if (segment instanceof EAKSegment) {
                            handleEAKSegment((EAKSegment) segment);
                        }
                        else if (segment instanceof ACKSegment) {
                            // do nothing.
                        }
                        else {
                            handleSegment(segment);
                        }

                        checkAndGetAck(segment);
                    }
                    catch (IllegalArgumentException xcp) {
                        /* A malformed segment says nothing about the rest of the
                           connection, so drop it and keep reading. */
                        xcp.printStackTrace();
                    }
                }
            }
            catch (IOException xcp) {
                xcp.printStackTrace();
            }
            catch (RuntimeException xcp) {
                /* This thread is the socket's only reader, so dying here would
                   leave callers blocked forever on a socket that still reports
                   itself connected. Fail the connection instead so they see it. */
                xcp.printStackTrace();
                connectionFailure();
            }
        }
    }

    private class NullSegmentTimerTask implements Runnable
    {
        public void run()
        {
            // Send a new NULL segment if there is nothing to be retransmitted.
            synchronized (_unackedSentQueue) {
                if (_unackedSentQueue.isEmpty()) {
                    try {
                        sendAndQueueSegment(new NULSegment(_counters.nextSequenceNumber()));
                    }
                    catch (IOException xcp) {
                        if (DEBUG) {
                            xcp.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /**
     * Runs one pass of the retransmission schedule: every unacknowledged
     * segment whose own deadline has expired is retransmitted once with its
     * timeout backed off, and the timer is re-armed for the next deadline.
     * <p>
     * The retransmission timer calls this when it fires. It is a method of its
     * own so that a test can drive the schedule from a clock it advances by
     * hand instead of waiting for real milliseconds to elapse, which is the
     * only way to tell a correct backoff from a missing one: both keep
     * retransmitting, only the instants differ.
     * <p>
     * Fails the connection, once the monitor has been released, if a segment
     * has run out of retransmissions.
     */
    protected void runRetransmissionPass()
    {
        boolean limitExceeded = false;
        long nowMillis = now();

        synchronized (_unackedSentQueue) {
            /*
             * Only the segments whose own deadline has expired are
             * retransmitted. Previously every unacknowledged segment was
             * resent on each tick, which with a large window turns a
             * single loss into a storm that congests the path even
             * further.
             */
            while (true) {
                Segment s = _retxQueue.peek();
                if (s == null || s.deadline() > nowMillis) {
                    break;
                }

                _retxQueue.poll();

                if (s.isAcked()) {
                    continue;
                }

                try {
                    s.backOffRto(RtoEstimator.MAX_BACKOFF_SHIFT);
                    limitExceeded = retransmitSegment(s);
                }
                catch (IOException xcp) {
                    xcp.printStackTrace();
                }

                if (limitExceeded) {
                    break;
                }
            }

            if (_unackedSentQueue.isEmpty()) {
                _retxQueue.clear();
                _retransmissionTimer.cancel();
            }
            else {
                rearmRetransmission();
            }
        }

        // Only called after _unackedSentQueue has been released, so that the
        // this -> _closeLock -> _unackedSentQueue lock order used by
        // close()/connectionFailure() is never violated.
        if (limitExceeded) {
            connectionFailure();
        }
    }

    private class RetransmissionTimerTask implements Runnable
    {
        public void run()
        {
            runRetransmissionPass();
        }
    }

    private class CumulativeAckTimerTask implements Runnable
    {
        public void run()
        {
            sendAck();
        }
    }

    private class KeepAliveTimerTask implements Runnable
    {
        public void run()
        {
            connectionFailure();
        }
    }

    private class ShutdownHook extends Thread
    {
        public ShutdownHook()
        {
            super("ReliableSocket-ShutdownHook");
        }

        public void run()
        {
            try {
                switch (_state) {
                    case CLOSED:
                        return;
                    default:
                        sendSegment(new FINSegment(_counters.nextSequenceNumber()));
                        break;
                }
            }
            catch (Throwable t) {
                // ignore exception
            }
        }
    }
}
