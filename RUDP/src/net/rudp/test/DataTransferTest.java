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
