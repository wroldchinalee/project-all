#### Netty总结

##### 编程范式

1. 创建服务类
2. 创建NIO Socket工厂
3. 创建管道工厂
4. 在管道上添加ChannelHandler
5. 绑定端口

##### 代码

```java
    public static void main(String[] args) {
        // 服务类
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        ExecutorService boss = Executors.newCachedThreadPool();
        ExecutorService worker = Executors.newCachedThreadPool();
        // 设置nio socket工厂
        serverBootstrap.setFactory(new NioServerSocketChannelFactory(boss, worker));
        // 创建管道工厂
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory(){
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new StringDecoder());
                pipeline.addLast("helloHandler", new HelloHandler());
                return pipeline;
            }
        });
        serverBootstrap.bind(new InetSocketAddress("localhost", 5555));
        System.out.println("netty服务器启动...");

    }
```

