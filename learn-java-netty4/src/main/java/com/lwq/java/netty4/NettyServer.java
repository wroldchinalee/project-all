package com.lwq.java.netty4;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author: LWQ
 * @create: 2020/11/21
 * @description: NettyServer
 **/
public class NettyServer {
    public static void main(String[] args) throws InterruptedException {
        // 创建两个线程组bossGroup和workerGroup，含有的子线程NioEventLoop的个数
        // 默认为cpu核数的两倍
        // bossGroup只是处理连接请求，真正的客户端业务处理会交给workerGroup完成
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            // 创建服务端的启动对象
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            // 设置两个线程组，使用NioServerSocketChannel作为服务器的通道实现
            serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    // 多个客户端同时来的时候，服务端将不能处理的客户端连接请求放在队列中等待处理
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    // 创建通道初始化对象，设置初始化参数
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            // 对workerGroup的SocketChannel设置处理器codec
                            socketChannel.pipeline().addLast(new NettyServerHandler());
                        }
                    });
            System.out.println("netty server start...");
            // 绑定一个端口并且同步，生成了一个ChannelFuture异步对象，通过isDone()等方法可以
            // 判断异步事件的执行情况
            // 启动服务器（并绑定端口），bind是异步操作，sync方法是等待异步操作执行完毕
            ChannelFuture channelFuture = serverBootstrap.bind(9000).sync();

            // 对通道关闭进行监听，closeFuture是异步操作，监听通道关闭
            // 通过sync方法同步等待通道关闭处理完成，这里会阻塞等待通道关闭完成
            channelFuture.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
