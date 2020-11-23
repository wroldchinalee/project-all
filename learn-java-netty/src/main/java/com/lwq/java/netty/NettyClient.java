package com.lwq.java.netty;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import java.net.InetSocketAddress;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: LWQ
 * @create: 2020/11/21
 * @description: NettyClient
 **/
public class NettyClient {
    public static void main(String[] args) {
        ClientBootstrap clientBootstrap = new ClientBootstrap();
        ExecutorService boss = Executors.newCachedThreadPool();
        ExecutorService worker = Executors.newCachedThreadPool();

        clientBootstrap.setFactory(new NioClientSocketChannelFactory(boss, worker));
        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new StringDecoder());
                pipeline.addLast("encoder", new StringEncoder());
                pipeline.addLast("handler", new HiHandler());
                return pipeline;
            }
        });

        ChannelFuture future = clientBootstrap.connect(new InetSocketAddress("127.0.0.1", 5555));
        Channel channel = future.getChannel();
        System.out.println("客户端启动...");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("请输入:");
            channel.write(scanner.next());
        }
    }
}
