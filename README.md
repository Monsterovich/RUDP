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

```
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


Javadoc
--
The javadoc of this project is available here:: https://build.germancoding.com/job/RUDP/javadoc/
