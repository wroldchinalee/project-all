package com.lwq.java.netty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by Administrator on 2020-11-19.
 * 这种方式可以处理多个客户端了，但是每来一个连接就需要分配一个线程来处理
 * 如果是短连接还好，处理完线程就释放了，但是如果要是长连接，那么线程资源
 * 就会一直被占用，而线程资源是有限的，所以连接数有限
 */
public class BIOClient {
    private int port;

    public BIOClient(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        System.out.println("客户端启动...");
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port));
        System.out.printf("客户端连接服务器，端口:%s...\n", port);
        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();
        Scanner scanner = new Scanner(System.in);
        byte[] bytes = new byte[1024];
        while (true) {
            while (scanner.hasNext()) {
                String message = scanner.next();
                System.out.println("客户端发送消息");
                outputStream.write(message.getBytes("UTF-8"));
                int length = inputStream.read(bytes);
                if (length > 0) {
                    System.out.printf("客户端接收消息:%s\n", new String(bytes, "UTF-8"));
                }
            }
        }

    }

    public static void main(String[] args) {
        BIOClient client = new BIOClient(5555);
        try {
            client.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
