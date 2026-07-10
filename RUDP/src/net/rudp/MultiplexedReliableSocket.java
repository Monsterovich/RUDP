/*
 * Simple Reliable UDP (rudp)
 * Copyright (c) 2026, Nikolay Borodin <monsterovich@gmail.com>
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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.util.ArrayList;

import net.rudp.impl.Segment;

/**
 * ReliableSocket for ACTIVE (outgoing) connections, which shares
 * a single DatagramSocket with ReliableServerSocket (or another
 * MultiplexedReliableSocket).
 *
 * Unlike a regular ReliableSocket, this class does not call
 * sock.receive() by itself - instead it receives segments via
 * segmentReceived(), which are passed to it by an external dispatcher
 * (in this design, ReceiverThread inside ReliableServerSocket, see
 * ReliableServerSocket.registerRoute()).
 *
 * To avoid a race condition between route registration and the arrival
 * of the SYN-ACK response, the preferred constructor takes a reference
 * to the ReliableServerSocket. In that case, connect() automatically
 * registers the route before sending the SYN, and close() automatically
 * unregisters it.
 *
 * Initialization order: the parent constructor (ReliableSocket) already
 * starts the segment reading thread (_sockThread) inside init(), so
 * _queue must be created not as a field initializer, but in the overridden
 * init() - before calling super.init(), which starts the thread. If
 * _queue is created as a regular field, there is a race condition risk:
 * the thread may access _queue before it is initialized.
 */
public class MultiplexedReliableSocket extends ReliableSocket implements PacketSink
{
    /**
     * Creates a socket for an outgoing connection over an existing
     * (already bound) shared DatagramSocket. The user is responsible
     * for manually registering the route before calling connect().
     *
     * @param sock the shared UDP socket, also used by
     *             ReliableServerSocket or other connections.
     */
    public MultiplexedReliableSocket(DatagramSocket sock)
    {
        super(sock);
        this._serverSocket = null;
    }

    public MultiplexedReliableSocket(DatagramSocket sock, ReliableSocketProfile profile)
    {
        super(sock, profile);
        this._serverSocket = null;
    }

    /**
     * Creates a socket with automatic route management.
     * When connect() is called, the socket registers itself with the server,
     * eliminating the race between registration and SYN-ACK arrival.
     * When close() is called, the route is automatically unregistered.
     *
     * @param server the ReliableServerSocket that dispatches incoming packets.
     */
    public MultiplexedReliableSocket(ReliableServerSocket server)
    {
        super(server.getUnderlyingSocket());
        this._serverSocket = server;
        // Without this listener, a route registered via registerRoute()/connect()
        // is only ever removed in close(). But a connection can also die via
        // connectionFailure() (max retransmissions exceeded, keep-alive timeout) -
        // a path close() never sees. When that happens, the entry in the server's
        // _clientSockTable is never cleaned up, and every subsequent SYN from the
        // same remote endpoint gets routed to this already-dead socket (whose
        // reading thread has already exited) instead of creating a fresh
        // ReliableClientSocket. The peer's new connection attempt is then
        // silently swallowed forever.
        addStateListener(new RouteCleanupListener());
    }

    public MultiplexedReliableSocket(ReliableServerSocket server, ReliableSocketProfile profile)
    {
        super(server.getUnderlyingSocket(), profile);
        this._serverSocket = server;
        addStateListener(new RouteCleanupListener());
    }

    protected void init(DatagramSocket sock, ReliableSocketProfile profile)
    {
        // The queue must be ready BEFORE super.init() starts the
        // reading thread (_sockThread.start()), otherwise NPE/race condition.
        _queue = new ArrayList<Segment>();
        super.init(sock, profile);
    }

    /**
     * Called by the internal thread of ReliableSocket (_sockThread) instead
     * of directly reading from the DatagramSocket. Blocks until the dispatcher
     * puts the next segment into the queue via segmentReceived().
     */
    protected Segment receiveSegmentImpl()
    {
        synchronized (_queue) {
            while (_queue.isEmpty() && !_localClosed) {
                try {
                    _queue.wait();
                }
                catch (InterruptedException xcp) {
                    xcp.printStackTrace();
                }
            }

            if (_localClosed && _queue.isEmpty()) {
                return null;
            }

            return (Segment) _queue.remove(0);
        }
    }

    /**
     * Called by the external dispatcher (ReliableServerSocket.ReceiverThread
     * via the routing table) when a new segment destined for our endpoint arrives.
     */
    public void segmentReceived(Segment s)
    {
        synchronized (_queue) {
            _queue.add(s);
            _queue.notify();
        }
    }

    /**
     * Closes ONLY this connection, without touching the shared DatagramSocket.
     * Clears the segment queue and signals the reading thread to terminate.
     * After closing, the route is automatically unregistered if a server
     * reference was provided.
     */
    protected void closeSocket()
    {
        synchronized (_queue) {
            // Clear any pending segments and insert null to wake up the thread
            _queue.clear();
            _queue.add(null);
            _localClosed = true;
            _queue.notify();
        }
    }

    /**
     * Initiates a connection to the specified endpoint.
     * If a server reference was set in the constructor, this method first
     * registers the route to prevent loss of the SYN-ACK reply.
     *
     * IMPORTANT: this overrides the TWO-argument connect(SocketAddress, int)
     * from ReliableSocket, not the one-argument connect(SocketAddress).
     * java.net.Socket declares both as separate overloads, and
     * ReliableSocket.connect(SocketAddress) is just a thin wrapper that
     * calls connect(endpoint, 0) - so overriding only the one-argument
     * version here would silently do nothing for any caller that invokes
     * the two-argument form directly (e.g. socket.connect(addr, timeoutMs)),
     * which is exactly what java.net.Socket-typed call sites tend to do.
     * In that case Java's virtual dispatch resolves straight to
     * ReliableSocket.connect(SocketAddress, int), completely bypassing
     * registerRoute() below - the SYN goes out, but the route table entry
     * for its reply is never created, so the SYN-ACK is misrouted into a
     * brand-new ReliableClientSocket instead of this socket's SYN_SENT
     * state machine, and this socket sits retransmitting its original SYN
     * forever while a orphaned duplicate session forms elsewhere.
     *
     * @param endpoint the remote address to connect to.
     * @param timeout  connect timeout in milliseconds (0 = no timeout).
     * @throws IOException if an I/O error occurs.
     */
    public void connect(SocketAddress endpoint, int timeout) throws IOException
    {
        // Cached separately from getRemoteSocketAddress(), because that method
        // returns null once the socket is no longer "connected" (e.g. after
        // connectionFailure() flips internal state) - exactly when the cleanup
        // listener needs it most.
        _connectEndpoint = endpoint;

        if (_serverSocket != null && endpoint != null) {
            if (_serverSocket.checkRoute(endpoint)) {
                throw new IOException("Already connected");
            }
            // Register before sending SYN to avoid race with incoming SYN-ACK
            _serverSocket.registerRoute(endpoint, this);
        }
        try {
            super.connect(endpoint, timeout);
        } catch (IOException e) {
            // If connection fails synchronously, remove the route we just added
            if (_serverSocket != null && endpoint != null) {
                _serverSocket.unregisterRoute(endpoint);
            }
            throw e;
        }
    }

    private ArrayList<Segment> _queue;

    // Reference to the server socket for automatic route registration/unregistration.
    // If null, the user must manually call registerRoute/unregisterRoute.
    private ReliableServerSocket _serverSocket;

    // Named _localClosed (not _closed) to avoid shadowing the private
    // _closed field from the parent ReliableSocket - both fields are distinct
    // and have different meanings, renamed to prevent confusion.
    private boolean _localClosed = false;

    // Endpoint passed to connect(), cached because getRemoteSocketAddress()
    // stops returning it once the socket is no longer considered connected.
    private SocketAddress _connectEndpoint;

    /**
     * Ensures the route registered in the server's routing table is removed
     * whenever this connection ends for ANY reason - not just an explicit
     * close(). Without this, a connection that dies via connectionFailure()
     * (retransmission limit / keep-alive timeout) leaves a permanently stale
     * entry in ReliableServerSocket._clientSockTable, silently blackholing
     * every future SYN from that same remote endpoint.
     */
    private class RouteCleanupListener implements ReliableSocketStateListener
    {
        public void connectionOpened(ReliableSocket sock)
        {
            // no-op
        }

        public void connectionRefused(ReliableSocket sock)
        {
            cleanup();
        }

        public void connectionClosed(ReliableSocket sock)
        {
            cleanup();
        }

        public void connectionFailure(ReliableSocket sock)
        {
            cleanup();
        }

        public void connectionReset(ReliableSocket sock)
        {
            // Reset does not necessarily end the connection, only close()/
            // failure/refusal do - see ReliableSocket.handleSegment(RSTSegment).
        }

        private void cleanup()
        {
            if (_serverSocket != null && _connectEndpoint != null) {
                _serverSocket.unregisterRoute(_connectEndpoint);
            }
        }
    }
}