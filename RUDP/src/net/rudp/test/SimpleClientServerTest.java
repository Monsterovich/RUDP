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

import net.rudp.ReliableServerSocket;
import net.rudp.ReliableSocket;

public class SimpleClientServerTest
{
    public static void main(String[] args) throws Exception
    {
        System.out.println("=== Simple ReliableSocket Client-Server Test ===\n");

        final int port = 9999;
        final String message = "Hello, RUDP!";

        Thread serverThread = new Thread(() -> {
            try {
                ReliableServerSocket serverSocket = new ReliableServerSocket(port);
                System.out.println("[Server] Listening on port " + port);

                Socket client = serverSocket.accept();
                System.out.println("[Server] Client connected: " + client.getRemoteSocketAddress());

                InputStream in = client.getInputStream();
                byte[] buffer = new byte[1024];
                int len = in.read(buffer);
                String received = new String(buffer, 0, len);
                System.out.println("[Server] Received: " + received);

                OutputStream out = client.getOutputStream();
                String response = "Echo: " + received;
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

        ReliableSocket clientSocket = new ReliableSocket();
        clientSocket.connect(new InetSocketAddress("127.0.0.1", port));
        System.out.println("[Client] Connected to server");

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
        System.out.println("[Client] Closed");

        serverThread.join();
        System.out.println("=== Test completed successfully ===");
    }
}
