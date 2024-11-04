package org.example.server;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

@Slf4j
public class TestServer {

    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private final List<String> data = List.of(
            "cat", "dog", "monkey", "monkey", "monkey", "monkey", "parrot", "cat", "cow", "dog", "fish", "octopus", "squid",
            "cat", "dog", "monkey", "parrot", "parrot", "parrot", "cat", "cow", "dog", "fish", "octopus", "squid"
    );

    public void start(int port) throws IOException, InterruptedException {
        serverSocket = new ServerSocket(port);
        clientSocket = serverSocket.accept();
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        for (String word : data) {
            Thread.sleep(1000);
            log.info("sending [{}]", word);
            out.println(word);
        }
    }

    public void stop() throws IOException {
        in.close();
        out.close();
        clientSocket.close();
        serverSocket.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        TestServer server = new TestServer();
        server.start(8888);
        server.stop();
    }
}
