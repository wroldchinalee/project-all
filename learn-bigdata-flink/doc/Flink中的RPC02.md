### Flink中的RPC02

##### RpcService和RpcServer

RpcService 和 RpcServer 是 RpcEndPoint 的成员变量。  

1） RpcService 是 Rpc 服务的接口，其主要作用如下：
⚫ 根据提供的 RpcEndpoint 来启动和停止 RpcServer（Actor）；
⚫ 根据提供的地址连接到(对方的)RpcServer，并返回一个 RpcGateway；
⚫ 延迟/立刻调度 Runnable、 Callable；  

在 Flink 中实现类为 AkkaRpcService， 是 Akka 的 **ActorSystem 的封装**，基本可以理解成 ActorSystem 的一个适配器。在 ClusterEntrypoint（JobMaster）和 TaskManagerRunner（TaskExecutor）启动的过程中初始化并启动。  

AkkaRpcService 中封装了 ActorSystem，并保存了 ActorRef 到 RpcEndpoint的映射关系。RpcService 跟 RpcGateway 类似，也提供了获取地址和端口的方法。  

在构造 RpcEndpoint 时会启动指定 rpcEndpoint 上的 RpcServer，其会根据 RpcEndpoint类型（FencedRpcEndpoint 或其他）来创建不同的 AkkaRpcActor（FencedAkkaRpcActor 或AkkaRpcActor），并将 RpcEndpoint和 AkkaRpcActor对应的 ActorRef保存起来，AkkaRpcActor是底层 Akka 调用的实际接收者，RPC 的请求在客户端被封装成 RpcInvocation 对象，以 Akka  消息的形式发送。  



**RpcService的创建：**

**ClusterEntrypoint.java**

```java
protected void initializeServices(Configuration configuration) throws Exception {
   LOG.info("Initializing cluster services.");
   synchronized (lock) {
      final String bindAddress = configuration.getString(JobManagerOptions.ADDRESS);
      final String portRange = getRPCPortRange(configuration);
      // TODO by lwq 初始化和启动AkkaRpcService，内部其实包装了一个ActorSystem
      commonRpcService = createRpcService(configuration, bindAddress, portRange);
      ...	...
   }
}
     
```

```java
private RpcService createRpcService(Configuration configuration, String bindAddress, String portRange) throws Exception {
	return AkkaRpcServiceUtils.createRpcService(bindAddress, portRange, configuration);
}
```

**AkkaRpcServiceUtils.java**

```java
public static RpcService createRpcService(
		String hostname,
		String portRangeDefinition,
		Configuration configuration) throws Exception {
    // 创建ActorSystem
	final ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, hostname, portRangeDefinition, LOG);
    // 创建AkkaRpcService
	return instantiateAkkaRpcService(configuration, actorSystem);
}
/**
```

```java
private static RpcService instantiateAkkaRpcService(Configuration configuration, ActorSystem actorSystem) {
	// 创建AkkaRpcService
    return new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.fromConfiguration(configuration));
}
```

创建好AkkaRpcService后传给了dispatcherResourceManagerComponentFactory的create方法

如下：

```java
private void runCluster(Configuration configuration) throws Exception {
    ...	...
	clusterComponent = dispatcherResourceManagerComponentFactory.create(
		configuration,
		ioExecutor,
		commonRpcService,
		haServices,
		blobServer,
		heartbeatServices,
		metricRegistry,
		archivedExecutionGraphStore,
		new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService()),
		this);
    ...	...
}
```



RpcEndpoint的构造方法中会startRpcServer，这里面还会根据不同的类型创建Actor，

代码如下：

```java
protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
	this.rpcService = checkNotNull(rpcService, "rpcService");
	this.endpointId = checkNotNull(endpointId, "endpointId");
	this.rpcServer = rpcService.startServer(this);
	this.mainThreadExecutor = new MainThreadExecutor(rpcServer, this::validateRunsInMainThread);
}
```



```java
public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
	checkNotNull(rpcEndpoint, "rpc endpoint");
	CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
	final Props akkaRpcActorProps;
    // 如果是FencedRpcEndpoint类型的Endpoint，就创建FencedAkkaRpcActor类型的actor
	if (rpcEndpoint instanceof FencedRpcEndpoint) {
		akkaRpcActorProps = Props.create(
			FencedAkkaRpcActor.class,
			rpcEndpoint,
			terminationFuture,
			getVersion(),
			configuration.getMaximumFramesize());
	} else {
        // 如果是其他类型，就创建AkkaRpcActor类型的actor
		akkaRpcActorProps = Props.create(
			AkkaRpcActor.class,
			rpcEndpoint,
			terminationFuture,
			getVersion(),
			configuration.getMaximumFramesize());
	}
	ActorRef actorRef;
	synchronized (lock) {
		checkState(!stopped, "RpcService is stopped");
        // 创建Actor
		actorRef = actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId());
		actors.put(actorRef, rpcEndpoint);
	}
	LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());
	final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
	final String hostname;
	Option<String> host = actorRef.path().address().host();
	if (host.isEmpty()) {
		hostname = "localhost";
	} else {
		hostname = host.get();
	}
	Set<Class<?>> implementedRpcGateways = new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.get
	implementedRpcGateways.add(RpcServer.class);
	implementedRpcGateways.add(AkkaBasedEndpoint.class);
	final InvocationHandler akkaInvocationHandler;
    // 如果是FencedRpcEndpoint类型的Endpoint，就创建FencedAkkaInvocationHandler类型的RpcServer
	if (rpcEndpoint instanceof FencedRpcEndpoint) {
		// a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
		akkaInvocationHandler = new FencedAkkaInvocationHandler<>(
			akkaAddress,
			hostname,
			actorRef,
			configuration.getTimeout(),
			configuration.getMaximumFramesize(),
			terminationFuture,
			((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken);
		implementedRpcGateways.add(FencedMainThreadExecutable.class);
	} else {
        // 如果是其他类型的Endpoint，就创建AkkaInvocationHandler类型的RpcServer
		akkaInvocationHandler = new AkkaInvocationHandler(
			akkaAddress,
			hostname,
			actorRef,
			configuration.getTimeout(),
			configuration.getMaximumFramesize(),
			terminationFuture);
	}
	// Rather than using the System ClassLoader directly, we derive the ClassLoader
	// from this class . That works better in cases where Flink runs embedded and all Flink
	// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
	ClassLoader classLoader = getClass().getClassLoader();
	@SuppressWarnings("unchecked")
    // 通过反射生成RpcServer
	RpcServer server = (RpcServer) Proxy.newProxyInstance(
		classLoader,
		implementedRpcGateways.toArray(new Class<?>[implementedRpcGateways.size()]),
		akkaInvocationHandler);
	return server;
}                                                                
```

**这里面很重要的一点是，RpcServer里面封装了AkkaRpcActor，这个Actor是自己的，而不是其他远程服务的。**



2） RpcServer 负责接收响应远端 RPC 消息请求， 自身的代理对象。有两个实现：
⚫ AkkaInvocationHandler
⚫ FencedAkkaInvocationHandler  

RpcServer 的启动是通知底层的 AkkaRpcActor 切换为 START 状态，开始处理远程调用
请求：  

```java
class AkkaInvocationHandler implements InvocationHandler, AkkaBasedEndpoint, RpcServer {
  @Override
  public void start() {
    // 这个rpcEndpoint对象是AkkaRpcActor，其实就是通知自己切换为Start状态
    rpcEndpoint.tell(ControlMessages.START, ActorRef.noSender());
  }
}
```

##### AkkaRpcActor  

AkkaRpcActor 是 Akka 的具体实现，主要负责处理如下类型消息：
1）本地 Rpc 调用 LocalRpcInvocation
会指派给 RpcEndpoint 进行处理，如果有响应结果，则将响应结果返还给 Sender。
2） RunAsync & CallAsync
这类消息带有可执行的代码，直接在 Actor 的线程中执行。
3）控制消息 ControlMessages
用来控制 Actor 行为， START 启动， STOP 停止，停止后收到的消息会丢弃掉。  

```java
public Receive createReceive() {
	return ReceiveBuilder.create()
		.match(RemoteHandshakeMessage.class, this::handleHandshakeMessage)
		.match(ControlMessages.class, this::handleControlMessage)
		.matchAny(this::handleMessage)
		.build();
}
```

**createReceive()方法是在receive方法内部调用的，其实就是receive方法的实现。**



#### RPC交互过程

RPC 通信过程分为请求和响应。  

##### 1.RPC 请求发送  

在 RpcService 中调用 connect()方法与对端的 RpcEndpoint （RpcServer）建立连接，connect()方 法 根 据 给 的 地 址 返 回 InvocationHandler(AkkaInvocationHandler 或FencedAkkaInvocationHandler，也就是对方的代理)。  
**AkkaRpcService.java**

```java
public <C extends RpcGateway> CompletableFuture<C> connect(
		final String address,
		final Class<C> clazz) {
	// TODO by lwq 返回值为RpcGateway，也就是一个rpc的代理对象
	return connectInternal(
		address,
		clazz,
		// TODO by lwq 工厂类
		(ActorRef actorRef) -> {
			Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);
			return new AkkaInvocationHandler(
				addressHostname.f0,
				addressHostname.f1,
				actorRef,
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				null);
		});
}
```

我们以JobManager为例，来看一下请求发送的流程：

在JobMaster是如何连接ResourceManager的。

**JobMaster.java**

```java
private void connectToResourceManager() {
	assert(resourceManagerAddress != null);
	assert(resourceManagerConnection == null);
	assert(establishedResourceManagerConnection == null);
	log.info("Connecting to ResourceManager {}", resourceManagerAddress);
	resourceManagerConnection = new ResourceManagerConnection(
		log,
		jobGraph.getJobID(),
		resourceId,
		getAddress(),
		getFencingToken(),
		resourceManagerAddress.getAddress(),
		resourceManagerAddress.getResourceManagerId(),
		scheduledExecutorService);
	// TODO by lwq
	resourceManagerConnection.start();
}
```

**RegisteredRpcConnection.java**

```java
public void start() {
   checkState(!closed, "The RPC connection is already closed");
   checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");
   // TODO by lwq
   final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
   if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
      // TODO by lwq
      newRegistration.startRegistration();
   } else {
      // concurrent start operation
      newRegistration.cancel();
   }
}
```

先看一下createNewRegistration()这个方法，返回值有三个泛型，其中G代表Gateway类型。

```java
final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
```

这个从类的声明中可以看到

```java
public abstract class RegisteredRpcConnection<F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success> {...}
```

```java
private RetryingRegistration<F, G, S> createNewRegistration() {
		// TODO by lwq
		RetryingRegistration<F, G, S> newRegistration = checkNotNull(generateRegistration());
    ... ...
}      
```

**JobMaster.java**

```java
protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
	return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess>(
		log,
		getRpcService(),
		"ResourceManager",
		ResourceManagerGateway.class,
		getTargetAddress(),
		getTargetLeaderId(),
		jobMasterConfiguration.getRetryingRegistrationConfiguration()) {
		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
			Time timeout = Time.milliseconds(timeoutMillis);
			return gateway.registerJobManager(
				jobMasterId,
				jobManagerResourceID,
				jobManagerRpcAddress,
				jobID,
				timeout);
		}
	};
}
```

这里面的泛型类型对应如下：

F:声明为Serializable类型，实现为ResourceManagerId

G:声明为RpcGateway类型，实现为ResourceManagerGateway

S:声明为RegistrationResponse.Success类型，实现为JobMasterRegistrationSuccess

