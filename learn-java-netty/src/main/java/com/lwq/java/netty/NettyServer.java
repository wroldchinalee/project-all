package com.lwq.java.netty;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: LWQ
 * @create: 2020/11/21
 * @description: NettyServer
 **/
public class NettyServer {
    public static void main(String[] args) {
        // 服务类
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        ExecutorService boss = Executors.newCachedThreadPool();
        ExecutorService worker = Executors.newCachedThreadPool();
        // 设置nio socket工厂
        serverBootstrap.setFactory(new NioServerSocketChannelFactory(boss, worker));
        // 创建管道工厂
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new StringDecoder());
                pipeline.addLast("encoder", new StringEncoder());
                pipeline.addLast("helloHandler", new HelloHandler());
                return pipeline;
            }
        });
        serverBootstrap.bind(new InetSocketAddress("127.0.0.1", 5555));
        System.out.println("netty服务器启动...");

    }
}
