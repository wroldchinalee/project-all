package com.lwq.java.netty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Administrator on 2020-11-18.
 * 这种最传统的IO方式，在两个地方都会阻塞
 * 一个是serverSocket.accept()
 * 另一个是inputStream.read(bytes)
 * 如果再有客户端进行连接也进不来，这样显然不能接受
 */
public class BIOServer {
    private int port;

    public BIOServer(int port) {
        this.port = port;
    }

    public void start() {
        ServerSocket serverSocket = null;
        Socket socket = null;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress("127.0.0.1", port));
            System.out.println("服务器启动...");
            System.out.printf("服务器绑定端口:%d...\n", port);
            while (true) {
                // 如果没有新连接，主线程会一直阻塞在这里
                socket = serverSocket.accept();
                System.out.println("服务器接收新连接...");
                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();
                byte[] bytes = new byte[1024];
                // 如果没有数据，会一直阻塞在read方法
                while (inputStream.read(bytes) > 0) {
                    String message = new String(bytes, "UTF-8");
                    System.out.printf("服务器接收消息:%s\n", message);
                    outputStream.write("success".getBytes("UTF-8"));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        BIOServer bioServer = new BIOServer(5555);
        bioServer.start();
    }
}
