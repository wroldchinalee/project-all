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
            // 获得一个ServerSocket通道
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            // 设置通道为非阻塞
            serverSocketChannel.configureBlocking(false);
            // 获得一个Selector
            Selector selector = Selector.open();
            // 将该通道对应的ServerSocket绑定到端口
            serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", port));
            // 将通道与Selector绑定，并为该通道注册OP_ACCEPT事件
            // 注册该事件后，当该事件到达时，select.select()会返回，否则会一直阻塞
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                // 当注册的事件到达时，方法返回；否则，该方法会一直阻塞
                selector.select();
                // 获得selector中选中的项的迭代器，选中的项为已注册事件并且已经就绪
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    // 删除已选的key，以防重复处理
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
                int length = channel.read(byteBuffer);
                if (length > 0) {
                    String message = new String(byteBuffer.array(), "UTF-8");
                    System.out.printf("服务端接收消息:%s\n", message);
                    ByteBuffer outBuffer = ByteBuffer.wrap("success".getBytes("UTF-8"));
                    channel.write(outBuffer);
                } else {
                    // 如果不判断长度小于0，客户端关闭后，会一直读就绪，然后抛异常
                    System.out.println("客户端关闭");
                    selectionKey.cancel();
                }

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
