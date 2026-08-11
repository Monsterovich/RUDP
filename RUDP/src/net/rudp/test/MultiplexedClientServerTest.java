package net.rudp.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;

import net.rudp.MultiplexedReliableSocket;
import net.rudp.ReliableServerSocket;

public class MultiplexedClientServerTest
{
    public static void main(String[] args) throws Exception
    {
        System.out.println("=== MultiplexedReliableSocket Client-Server Test ===\n");
        System.out.println("Client uses multiplexed socket to connect to remote server\n");

        final int serverPort = 8888;
        final int clientLocalPort = 8889;
        final String message = "Hello from multiplexed client!";

        Thread serverThread = new Thread(() -> {
            try {
                ReliableServerSocket serverSocket = new ReliableServerSocket(serverPort);
                System.out.println("[Server] Listening on port " + serverPort);

                Socket client = serverSocket.accept();
                System.out.println("[Server] Client connected: " + client.getRemoteSocketAddress());

                InputStream in = client.getInputStream();
                byte[] buffer = new byte[1024];
                int len = in.read(buffer);
                String received = new String(buffer, 0, len);
                System.out.println("[Server] Received: " + received);

                OutputStream out = client.getOutputStream();
                String response = "Server echo: " + received;
                out.write(response.getBytes());
                out.flush();
                System.out.println("[Server] Sent response: " + response);

                client.close();
                serverSocket.close();
                System.out.println("[Server] Closed\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        serverThread.start();
        Thread.sleep(500);

        DatagramSocket clientUdpSocket = new DatagramSocket(null);
        clientUdpSocket.setReuseAddress(true);
        clientUdpSocket.bind(new InetSocketAddress(clientLocalPort));
        System.out.println("[Client] UDP socket bound to port " + clientLocalPort);

        ReliableServerSocket clientServerSocket = new ReliableServerSocket(clientUdpSocket, 0);
        MultiplexedReliableSocket clientSocket = new MultiplexedReliableSocket(clientServerSocket);

        InetSocketAddress serverAddr = new InetSocketAddress("127.0.0.1", serverPort);
        clientSocket.connect(serverAddr, 5000);
        System.out.println("[Client] Connected to server on port " + serverPort);

        OutputStream out = clientSocket.getOutputStream();
        out.write(message.getBytes());
        out.flush();
        System.out.println("[Client] Sent: " + message);

        InputStream in = clientSocket.getInputStream();
        byte[] buffer = new byte[1024];
        int len = in.read(buffer);
        String response = new String(buffer, 0, len);
        System.out.println("[Client] Received: " + response);

        clientSocket.close();
        clientServerSocket.close();
        System.out.println("[Client] Closed");

        serverThread.join();
        System.out.println("=== Test completed successfully ===");
    }
}
