# RUDP
A (slightly) modified version of the RUDP libary written by Adrian Granados (not me, copyright for this goes to him. See RUDP/license.txt for details).

R-UDP means "Reliable UDP". A description of the protocol can be found here https://datatracker.ietf.org/doc/html/draft-ietf-sigtran-reliable-udp-00

Download
--
You can download a up to date pre-compiled version of this API here (Compiled with Java 11):
https://build.germancoding.com/job/RUDP/lastSuccessfulBuild/artifact/RUDP/rudp-SNAPSHOT.jar

Of course you can also download/clone the source and compile it for yourself. There are no dependencies, the project was tested working on Java 6/7/8.

Usage
--
The design of the library is very similar (if not equal) to the Java socket API. To create a new connection with RUDP, do the same thing you would do with Java TCP/IO. The library will handle most stuff in the background, just like TCP.

```java
// For the server
ReliableServerSocket serverSocket = new ReliableServerSocket(0);
Socket someRUDPClient = serverSocket.accept(); 

// For the client
ReliableSocket client = new ReliableSocket();
client.connect(serverSocket.getLocalSocketAddress()); 
/* This is just an example, for a real tutorial look up Java sockets */
```
I also added an option to use pre-existing (and already bound) UDP sockets for the RUDP connection, just pass them as parameter to the constructor.

Multiplexing support
--

This fork supports port multiplexing: `ReliableServerSocket` and one or more externally-created `PacketSink` implementations (e.g. `MultiplexedReliableSocket` for outbound connections) can now share the same underlying `DatagramSocket`.

Here's an example of how I use this in my app. By the way, this implementation is compatible with the previous ReliableSocket. I just need to connect to and receive packets from a single port to bypass Port Restricted NAT. See https://github.com/GermanCoding/RUDP/issues/6

<img width="889" height="293" alt="wireshark dump" src="https://github.com/user-attachments/assets/6ce4eeed-1386-4ffd-91a4-a4156e062139" />


Server socket:

```java
DatagramSocket sock = new DatagramSocket(null);
sock.setReuseAddress(true); // previously used by STUN client
sock.bind(new InetSocketAddress(serverPort));
ServerSocket server = new ReliableServerSocket(sock, 0);
serverLatch.countDown();
```

Client socket (with new `MultiplexedReliableSocket`).

```java
serverLatch.await(); // wait for the server object to be initialized, since the server and client run in different threads
ReliableServerSocket reliableServer = (ReliableServerSocket)server;
socket = new MultiplexedReliableSocket(reliableServer);

socket.connect(new InetSocketAddress(host, port), SOCKET_TIMEOUT_MS);
```


Upstream fixes and optimizations
--

This fork includes a number of bug fixes and performance improvements that are not present
in the original [GermanCoding/RUDP](https://github.com/GermanCoding/RUDP) (upstream ends
at `28ed965`).

### Bug fixes

- **Malformed segments no longer silently kill the reader thread.** Bad packets (wrong
  length, truncated data, invalid flags) are now rejected with `IllegalArgumentException`
  instead of causing `ArrayIndexOutOfBoundsException` deep in the receive path.
- **Stale `_clientSockTable` entries on connection failure.** Failed connections left
  dangling entries in the client socket table, causing heap growth.
- **Shutdown hook leak on connection failure.** `addShutdownHook()` was called on every
  connection attempt but never removed on failure, causing permanent heap leaks.
- **Half-open connections stuck in `SYN_RCVD`.** Incoming connections that never completed
  the handshake were never cleaned up, leaking sockets indefinitely.
- **RST packet resets keep-alive watchdog.** A stray RST would reset the keep-alive timer,
  preventing zombie connection cleanup. Now RST closes immediately without touching the
  watchdog.
- **Potential deadlock in `connectionFailure()`.** Synchronization order between
  `_listeners` and connection state could deadlock when callbacks triggered socket
  operations.
- **Thread leaks on socket close.** Several code paths on `close()` failed to interrupt
  the retransmission timer thread.

### Performance and protocol improvements

- **Per-segment retransmission scheduling.** Replaced the single fixed-interval retransmission
  tick with per-segment deadlines, so each segment tracks its own expiry independently.
- **RTT estimation and RTO calculation (RFC 6298).** Added proper smoothed RTT and RTT
  deviation tracking with exponential weighted moving averages. The RTO estimator is now
  a pure, testable object (`RtoEstimator`) with configurable clock injection.
- **Fast retransmit.** Segments are retransmitted on triple duplicate ACK without waiting
  for the RTO timer.
- **EAK hole recovery with backoff.** Explicit Acknowledgment gaps now retransmit only
  the first segment of a hole (not the entire hole), with incremental backoff between
  retries throttled to once per RTO.
- **Reno-style congestion control.** Slow start (`cwnd += 1` per ACK), congestion avoidance
  (`cwnd += 1/cwnd` per ACK), fast retransmit drops `cwnd` to 1, timeout drops to
  `ssthresh = max(cwnd/2, 2)`. EAK recovery drops `cwnd = ssthresh` (not 1) to avoid
  choking the send window. The effective send window is `min(maxOutstandingSegs, cwnd)`.
- **Injectable clock.** `ReliableSocket` accepts a `Clock` implementation via constructor,
  enabling deterministic testing of timeouts and retransmission logic.

These changes together deliver roughly **7–10x throughput improvement** on loopback
(16 MB payload, ~160 MB/s vs ~22 MB/s upstream), with no measurable latency penalty.
The biggest gain came from fixing the retransmission storm, not micro-optimisations.

Javadoc
--
The javadoc of this project is available here:: https://build.germancoding.com/job/RUDP/javadoc/
