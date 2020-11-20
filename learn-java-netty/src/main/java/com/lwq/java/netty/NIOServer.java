package com.lwq.java.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Administrator on 2020-11-19.
 */
public class NIOServer {
    private int port;

    public NIOServer(int port) {
        this.port = port;
    }

    public void start() {
        System.out.println("服务器端启动...");
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            Selector selector = Selector.open();
            serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", port));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();
                    handle(selectionKey, selector);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handle(SelectionKey selectionKey, Selector selector) {
        if (selectionKey.isAcceptable()) {
            ServerSocketChannel channel = ((ServerSocketChannel) selectionKey.channel());
            try {
                SocketChannel socketChannel = channel.accept();
                System.out.println("服务端接收新连接...");
                socketChannel.configureBlocking(false);
                socketChannel.register(selector, SelectionKey.OP_READ);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (selectionKey.isReadable()) {
            SocketChannel channel = (SocketChannel) selectionKey.channel();
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            try {
                int read = channel.read(byteBuffer);
                String message = new String(byteBuffer.array(), "UTF-8");
                System.out.printf("服务端接收消息:%s\n", message);
                byteBuffer.clear();
                byteBuffer.put("success".getBytes("UTF-8"));
                channel.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        NIOServer nioServer = new NIOServer(5555);
        nioServer.start();
    }
}
