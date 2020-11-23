package com.lwq.java.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Created by Administrator on 2020-11-19.
 */
public class NIOServerWithReadNoData {
    private int port;
    private SocketChannel socketChannel;

    public NIOServerWithReadNoData(int port) {
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
                selector.select(1000);
                // 获得selector中选中的项的迭代器，选中的项为已注册事件并且已经就绪
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();
                    // 删除已选的key，以防重复处理
                    handle(selectionKey, selector);
                }
                // 如果连接成功，socketChannel不为null的情况，直接调用它的read方法
                // 可以看到在没有数据的情况，直接返回结果，读取到了0个字节
                if (socketChannel != null) {
                    ByteBuffer allocate = ByteBuffer.allocate(1024);
                    try {
                        int read = socketChannel.read(allocate);
                        System.out.printf("读取%d个字节\n", read);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
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
                this.socketChannel = socketChannel;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        NIOServerWithReadNoData nioServer = new NIOServerWithReadNoData(5555);
        nioServer.start();
    }
}
