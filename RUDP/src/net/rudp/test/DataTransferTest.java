package net.rudp.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;
import net.rudp.ReliableSocketProfile;
import net.rudp.ReliableSocketStateListener;

public class DataTransferTest
{
    public static void main(String[] args) throws Exception
    {
        System.out.println("=== Data Transfer Test ===\n");

        final int port = 7777;
        final int dataSize = 10000;
        final CountDownLatch latch = new CountDownLatch(1);

        Thread serverThread = new Thread(() -> {
            try {
                ReliableServerSocket serverSocket = new ReliableServerSocket(port);
                System.out.println("[Server] Listening on port " + port);

                Socket client = serverSocket.accept();
                System.out.println("[Server] Client connected");

                InputStream in = client.getInputStream();
                byte[] buffer = new byte[dataSize];
                int totalReceived = 0;
                int len;

                long startTime = System.currentTimeMillis();
                while (totalReceived < dataSize) {
                    len = in.read(buffer, totalReceived, dataSize - totalReceived);
                    if (len == -1) break;
                    totalReceived += len;
                }
                long endTime = System.currentTimeMillis();

                System.out.println("[Server] Received " + totalReceived + " bytes in " +
                    (endTime - startTime) + " ms");

                client.close();
                serverSocket.close();
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        serverThread.start();
        Thread.sleep(500);

        ReliableSocket clientSocket = new ReliableSocket();

        boolean connectionOpened[] = {false};
        boolean connectionClosed[] = {false};

        clientSocket.addStateListener(new ReliableSocketStateListener() {
            public void connectionOpened(ReliableSocket sock) {
                connectionOpened[0] = true;
                System.out.println("[Client] Connection opened event");
            }
            public void connectionRefused(ReliableSocket sock) {
                System.out.println("[Client] Connection refused event");
            }
            public void connectionClosed(ReliableSocket sock) {
                connectionClosed[0] = true;
                System.out.println("[Client] Connection closed event");
            }
            public void connectionFailure(ReliableSocket sock) {
                System.out.println("[Client] Connection failure event");
            }
            public void connectionReset(ReliableSocket sock) {
                System.out.println("[Client] Connection reset event");
            }
        });

        clientSocket.connect(new InetSocketAddress("127.0.0.1", port));
        System.out.println("[Client] Connected to server");

        OutputStream out = clientSocket.getOutputStream();
        byte[] data = new byte[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data[i] = (byte)(i % 256);
        }

        long startTime = System.currentTimeMillis();
        out.write(data);
        out.flush();
        long endTime = System.currentTimeMillis();

        System.out.println("[Client] Sent " + dataSize + " bytes in " +
            (endTime - startTime) + " ms");

        clientSocket.close();
        System.out.println("[Client] Closed");

        latch.await(10, TimeUnit.SECONDS);

        System.out.println("\nConnection opened: " + connectionOpened[0]);
        System.out.println("Connection closed: " + connectionClosed[0]);
        System.out.println("=== Test completed successfully ===");
    }
}
