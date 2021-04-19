### Flink中的RPC

![image-20210411151312668](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210411151312668.png)



#### 组件介绍

##### RpcGateway

Flink 的 RPC 协议通过 RpcGateway 来定义，**主要定义通信行为**； 用于远程调用RpcEndpoint 的某些方法， 可以理解为**对方的客服端代理**。
若想与远端 Actor 通信，则必须提供地址（ip 和 port），如在 Flink-on-Yarn 模式下，JobMaster 会先启动 ActorSystem，此时 TaskExecutor 的 Container 还未分配，后面与TaskExecutor 通信时，必须让其提供对应地址。  

![RpcGateway](E:\source_code\project-all\learn-bigdata-flink\doc\RpcGateway.png)

从类继承图可以看到基本上所有组件都实现了 RpcGateway 接口，其代码如下：  

```java
public interface RpcGateway {

	// 远端服务的地址
	String getAddress();

    // 远端服务的主机名
	String getHostname();
}
```

##### RpcEndpoint

RpcEndpoint 是通信终端， **提供 RPC 服务组件的生命周期管理(start、 stop)。** 每个RpcEndpoint对应了一个路径（endpointId和 actorSystem共同确定），每个路径对应一个 Actor，其实现了 RpcGateway 接口，其构造函数如下：  

```java
protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
	this.rpcService = checkNotNull(rpcService, "rpcService");
	this.endpointId = checkNotNull(endpointId, "endpointId");
    // 在这里会构建RpcServer
	this.rpcServer = rpcService.startServer(this);
    // 主线程执行器，所有调用在主线程中串行执行
	this.mainThreadExecutor = new MainThreadExecutor(rpcServer, this::validateRunsInMainThread);
}
```

构造的时候调用 rpcService.startServer()创建RpcServer，在调用RpcEndpoint的start()方法时，启动RpcServer，进入可以接收处理请求的状态，最后将 RpcServer 绑定到主线程上真正执行起来。
在 RpcEndpoint 中还定义了一些方法如 runAsync(Runnable)、 callAsync(Callable, Time)方法来执行 Rpc 调用，值得注意的是**在 Flink 的设计中，对于同一个 Endpoint，所有的调用都运行在主线程，因此不会有并发问题**，当启动 RpcEndpoint/进行 Rpc 调用时，其会委托RcpServer 进行处理。  

**JobMaster.java**

```java
public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
	// make sure we receive RPC and async calls
	start();
	//TODO by lwq 调用startJobExecution
	return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
}
```

```java
public final void start() {
   //TODO by lwq 调用rpcServer的start方法，最终会调用这个rpcEndpoint的onStart方法
   //TODO by lwq 如果是dispatcher，直接跳到Dispatcher的onStart方法
   //TODO by lwq 如果是taskManager，直接跳到TaskExecutor的onStart方法
   rpcServer.start();
}
```

**AkkaInvocationHandler.java**

```java
public void start() {
	rpcEndpoint.tell(ControlMessages.START, ActorRef.noSender());
}
```

如果是启动的方法，会调用AkkaRpcActor的createMessage方法，createMessage方法是AbstractActor对receive方法的实现。

**AkkaRpcActor.java**

```java
public Receive createReceive() {
	return ReceiveBuilder.create()
		.match(RemoteHandshakeMessage.class, this::handleHandshakeMessage)
		.match(ControlMessages.class, this::handleControlMessage)
		.matchAny(this::handleMessage)
		.build();
}
```

这个方法会根据消息类型来决定如何处理。

有三种类型；

1.RemoteHandshakeMessage：握手消息，远程的actor建立连接

2.ControlMessages：启动和停止等消息

3.其他消息

start属于ControlMessages，直接看handleControlMessage方法。

```java
private void handleControlMessage(ControlMessages controlMessage) {
	try {
		switch (controlMessage) {
			case START:
				state = state.start(this);
				break;
			case STOP:
				state = state.stop();
				break;
			case TERMINATE:
				state = state.terminate(this);
				break;
			default:
				handleUnknownControlMessage(controlMessage);
		}
	} catch (Exception e) {
		this.rpcEndpointTerminationResult = RpcEndpointTerminationResult.failure(e);
		throw e;
	}
}
```

```java
enum StartedState implements State {
		STARTED;
		
    	// 启动直接返回状态
		@Override
		public State start(AkkaRpcActor<?> akkaRpcActor) {
			return STARTED;
		}

	...	...

		@Override
		public boolean isRunning() {
			return true;
		}
	}
```

可以看到，启动方法最终就是把AkkaRpcActor中的state修改为STARTED状态。