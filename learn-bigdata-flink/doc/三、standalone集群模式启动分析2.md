### 三、standalone集群模式启动分析2-JobManager启动

这一章分析JobManager启动类StandaloneSessionClusterEntrypoint。

#### 一、StandaloneSessionClusterEntrypoint

先上代码：

```java
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

	public StandaloneSessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(StandaloneResourceManagerFactory.INSTANCE);
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		EntrypointClusterConfiguration entrypointClusterConfiguration = null;
		final CommandLineParser<EntrypointClusterConfiguration> commandLineParser = new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

		try {
			entrypointClusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneSessionClusterEntrypoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

		StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}
}
```

上面代码执行的核心流程如下：

```java
// 入口
|-- StandaloneSessionClusterEntrypoint.main()
| |-- ClusterEntrypoint.runClusterEntrypoint(entrypoint)
| | |-- ClusterEntrypoint.startCluster()
| | | |-- runCluster(configuration)
// 初始化各种服务
| | | | |-- initializeServices(configuration)
// 初始化各种组件的工厂实例
| | | | |-- createDispatcherResourceManagerComponentFactory(configuration)
// 创建一些集群组件
| | | | |-- dispatcherResourceManagerComponentFactory.create(...)
```



重点分析initializeServices(configuration)，createDispatcherResourceManagerComponentFactory(configuration)和dispatcherResourceManagerComponentFactory.create(...)方法

#### 二、initializeServices方法

initializeServices方法代码如下：

```java
	protected void initializeServices(Configuration configuration) throws Exception {

		LOG.info("Initializing cluster services.");

		synchronized (lock) {
			final String bindAddress = configuration.getString(JobManagerOptions.ADDRESS);
			final String portRange = getRPCPortRange(configuration);
			// 1.初始化和启动AkkaRpcService，内部其实包装了一个ActorSystem
			commonRpcService = createRpcService(configuration, bindAddress, portRange);

			// update the configuration used to create the high availability services
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());
			// 2.初始化一个负责IO的线程池
			ioExecutor = Executors.newFixedThreadPool(
				Hardware.getNumberCPUCores(),
				new ExecutorThreadFactory("cluster-io"));
            // 3.初始化HA服务，负责HA服务的是：ZooKeeperHaServices
			haServices = createHaServices(configuration, ioExecutor);
            // 4.初始化BlobServer服务端
			blobServer = new BlobServer(configuration, haServices.createBlobStore());
			blobServer.start();
            // 5.初始化心跳服务
			heartbeatServices = createHeartbeatServices(configuration);
            // 6.初始化统计服务
			metricRegistry = createMetricRegistry(configuration);

			final RpcService metricQueryServiceRpcService = MetricUtils.startMetricsRpcService(configuration, bindAddress);
			metricRegistry.startQueryService(metricQueryServiceRpcService, null);

			final String hostname = RpcUtils.getHostname(commonRpcService);
		
			processMetricGroup = MetricUtils.instantiateProcessMetricGroup(
				metricRegistry,
				hostname,
				ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));
			// 7.初始化一个用来存储ExecutionGraph的Store，实现是FileArchivedExecutionGraphStore
			archivedExecutionGraphStore = createSerializableExecutionGraphStore(configuration, commonRpcService.getScheduledExecutor());
		}
	}
```



#### 三、createDispatcherResourceManagerComponentFactory(configuration)方法

createDispatcherResourceManagerComponentFactory(configuration)方法代码如下：

```java
protected abstract DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException;
```

它实际上是一个抽象方法，实现是在StandaloneSessionClusterEntrypoint类中，代码如下：

```java
protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(StandaloneResourceManagerFactory.INSTANCE);
	}
```

这个方法里面调用的是DefaultDispatcherResourceManagerComponentFactory的静态方法createSessionComponentFactory

代码如下：

```java
public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
			ResourceManagerFactory<?> resourceManagerFactory) {
		return new DefaultDispatcherResourceManagerComponentFactory(
			DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE),
			resourceManagerFactory,
			SessionRestEndpointFactory.INSTANCE);
	}
```

可以看到这个方法里面就是创建了一个DefaultDispatcherResourceManagerComponentFactory对象。

我们再看一下它的构造方法

```java
	DefaultDispatcherResourceManagerComponentFactory(
			@Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
			@Nonnull ResourceManagerFactory<?> resourceManagerFactory,
			@Nonnull RestEndpointFactory<?> restEndpointFactory) {
		this.dispatcherRunnerFactory = dispatcherRunnerFactory;
		this.resourceManagerFactory = resourceManagerFactory;
		this.restEndpointFactory = restEndpointFactory;
	}
```

这段代码就很简单了，就是给DefaultDispatcherResourceManagerComponentFactory对象里面的成员变量赋值，这里面的成员变量就是三个工厂类对象。

这个三个工厂类分别是：

> ```java
> DispatcherRunnerFactory
> ResourceManagerFactory
> RestEndpointFactory
> ```

再看一下三个工厂类对象是如何创建的？

先看第一个DispatcherRunnerFactory，它是通过下面这行代码创建的：

```java
DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE)
```

它里面的实现逻辑如下：

```java
	public static DefaultDispatcherRunnerFactory createSessionRunner(DispatcherFactory dispatcherFactory) {
		return new DefaultDispatcherRunnerFactory(
			SessionDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
	}
```



再看第二个ResourceManagerFactory是如何创建的

它是通过这行代码传进来的

```java
StandaloneResourceManagerFactory.INSTANCE
```

它其实就是一个枚举类，里面定义了这个工厂类如何创建它的产品对象

代码如下：

```java
public enum StandaloneResourceManagerFactory implements ResourceManagerFactory<ResourceID> {
	INSTANCE;

	@Override
	public ResourceManager<ResourceID> createResourceManager(...) throws Exception {
    		省略代码......
    }
}
```



最后一个RestEndpointFactory是如何创建的

它是通过这行代码获得的

```java
SessionRestEndpointFactory.INSTANCE
```

它也是一个枚举类，里面定义了这个工厂类如何创建它的产品对象

```java
public enum SessionRestEndpointFactory implements RestEndpointFactory<DispatcherGateway> {
	INSTANCE;

	@Override
	public WebMonitorEndpoint<DispatcherGateway> createRestEndpoint(...) throws Exception {
			省略代码......
	}
}
```

**总结一下这个方法的主要作用就是：**

**1.创建DefaultDispatcherResourceManagerComponentFactory对象**

**2.初始化DefaultDispatcherResourceManagerComponentFactory对象里面的三个成员变量DispatcherRunnerFactory，ResourceManagerFactory和RestEndpointFactory。**

它们的实现分别是：

| 抽象类                  | 具体类                           |
| ----------------------- | -------------------------------- |
| DispatcherRunnerFactory | DefaultDispatcherRunnerFactory   |
| ResourceManagerFactory  | StandaloneResourceManagerFactory |
| RestEndpointFactory     | SessionRestEndpointFactory       |



#### 四、dispatcherResourceManagerComponentFactory.create(...)方法

DefaultDispatcherResourceManagerComponentFactory的create方法代码如下：

```java
public DispatcherResourceManagerComponent create(
			Configuration configuration,
			Executor ioExecutor,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		LeaderRetrievalService dispatcherLeaderRetrievalService = null;
		LeaderRetrievalService resourceManagerRetrievalService = null;
		WebMonitorEndpoint<?> webMonitorEndpoint = null;
		ResourceManager<?> resourceManager = null;
		ResourceManagerMetricGroup resourceManagerMetricGroup = null;
		DispatcherRunner dispatcherRunner = null;

		try {
			dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

			resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

			final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				DispatcherGateway.class,
				DispatcherId::fromUuid,
				10,
				Time.milliseconds(50L));

			final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				ResourceManagerGateway.class,
				ResourceManagerId::fromUuid,
				10,
				Time.milliseconds(50L));

			final ScheduledExecutorService executor = WebMonitorEndpoint.createExecutorService(
				configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
				configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
				"DispatcherRestEndpoint");

			final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
			final MetricFetcher metricFetcher = updateInterval == 0
				? VoidMetricFetcher.INSTANCE
				: MetricFetcherImpl.fromConfiguration(
					configuration,
					metricQueryServiceRetriever,
					dispatcherGatewayRetriever,
					executor);

            // 创建webMonitorEndpoint
			webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				blobServer,
				executor,
				metricFetcher,
				highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
				fatalErrorHandler);

			log.debug("Starting Dispatcher REST endpoint.");
			webMonitorEndpoint.start();

			final String hostname = RpcUtils.getHostname(rpcService);

			resourceManagerMetricGroup = ResourceManagerMetricGroup.create(metricRegistry, hostname);
            // 创建resourcemanager
			resourceManager = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				fatalErrorHandler,
				new ClusterInformation(hostname, blobServer.getPort()),
				webMonitorEndpoint.getRestBaseUrl(),
				resourceManagerMetricGroup);

			final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, webMonitorEndpoint, ioExecutor);

			final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(
				configuration,
				highAvailabilityServices,
				resourceManagerGatewayRetriever,
				blobServer,
				heartbeatServices,
				() -> MetricUtils.instantiateJobManagerMetricGroup(metricRegistry, hostname),
				archivedExecutionGraphStore,
				fatalErrorHandler,
				historyServerArchivist,
				metricRegistry.getMetricQueryServiceGatewayRpcAddress());

            // 创建Dispather
			log.debug("Starting Dispatcher.");
			dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(
				highAvailabilityServices.getDispatcherLeaderElectionService(),
				fatalErrorHandler,
				new HaServicesJobGraphStoreFactory(highAvailabilityServices),
				ioExecutor,
				rpcService,
				partialDispatcherServices);

			log.debug("Starting ResourceManager.");
			resourceManager.start();

			resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

			return new DispatcherResourceManagerComponent(
				dispatcherRunner,
				resourceManager,
				dispatcherLeaderRetrievalService,
				resourceManagerRetrievalService,
				webMonitorEndpoint);

		} catch (Exception exception) {
			// clean up all started components
			if (dispatcherLeaderRetrievalService != null) {
				try {
					dispatcherLeaderRetrievalService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
			}

			if (resourceManagerRetrievalService != null) {
				try {
					resourceManagerRetrievalService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
			}

			final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

			if (webMonitorEndpoint != null) {
				terminationFutures.add(webMonitorEndpoint.closeAsync());
			}

			if (resourceManager != null) {
				terminationFutures.add(resourceManager.closeAsync());
			}

			if (dispatcherRunner != null) {
				terminationFutures.add(dispatcherRunner.closeAsync());
			}

			final FutureUtils.ConjunctFuture<Void> terminationFuture = FutureUtils.completeAll(terminationFutures);

			try {
				terminationFuture.get();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (resourceManagerMetricGroup != null) {
				resourceManagerMetricGroup.close();
			}

			throw new FlinkException("Could not create the DispatcherResourceManagerComponent.", exception);
		}
	}
```

这个方法的代码比较长，核心的代码主要是这几句:

1.通过restEndpointFactory创建webMonitorEndpoint

```
webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				blobServer,
				executor,
				metricFetcher,
				highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
				fatalErrorHandler);
```

2.通过resourceManagerFactory创建resourceManager

```
resourceManager = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				fatalErrorHandler,
				new ClusterInformation(hostname, blobServer.getPort()),
				webMonitorEndpoint.getRestBaseUrl(),
				resourceManagerMetricGroup);
```

3.通过dispatcherRunnerFactory创建dispatcherRunner

```java
dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(
				highAvailabilityServices.getDispatcherLeaderElectionService(),
				fatalErrorHandler,
				new HaServicesJobGraphStoreFactory(highAvailabilityServices),
				ioExecutor,
				rpcService,
				partialDispatcherServices);
```

这3个工厂就是前面创建的，在这个方法里通过它们创建了各自的产品对象。

那么这几个对象的具体是哪几个类？

如下表格：

| 抽象类             | 具体类                    |
| ------------------ | ------------------------- |
| WebMonitorEndpoint | DispatcherRestEndpoint    |
| ResourceManager    | StandaloneResourceManager |
| DispatcherRunner   | DefaultDispatcherRunner   |

