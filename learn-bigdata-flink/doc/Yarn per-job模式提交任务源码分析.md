### Yarn per-job模式提交任务源码分析

**下面的源码分析基于flink1.10.1**

先看一张简单的流程图：

<div align=center>
  <img src="images/submitJob.png" />
</div>

简单地看，提交的流程主要有六步：

1. 通过client提交任务到yarn集群的ResourceManager
2. ResourceManager分配一个container来启动ApplicationMaster，也就是Flink的JobManager。JobManager主要有JobMaster，Dispatcher，ResourceManager等几个重要的组件
3. JobMaster向Flink的ResourceManager申请slot
4. Flink的RM会向Yarm的RM申请资源，分配container给flink，在container上启动TaskManager
5. TaskManager启动成功后会向Flink的RM注册
6. JobMaster将任务调度到分配好的TaskManager中执行任务

<div align=center>
  <img src="images/submitApplication流程.png" />
</div>



下面来详细分析每一步骤的代码实现

#### 1.提交前的执行逻辑

##### 1.1 执行启动脚本

flink脚本部署时位于bin目录下，如果是源码位于flink-dist模块src/main/flink-bin/bin目录下

flink脚本主要做了以下几件事：		

1. 加载config.sh
2. 
3. 调用java命令执行org.apache.flink.client.cli.CliFrontend类

```shell
exec $JAVA_RUN $JVM_ARGS $FLINK_ENV_JAVA_OPTS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.client.cli.CliFrontend "$@"
```



##### 1.2 CliFrontendParser解析参数

```java
	protected void run(String[] args) throws Exception {
		LOG.info("Running 'run' command.");

		final Options commandOptions = CliFrontendParser.getRunCommandOptions();
		final CommandLine commandLine = getCommandLine(commandOptions, args, true);
	}
```

```java
	public CommandLine getCommandLine(final Options commandOptions, final String[] args, final boolean stopAtNonOptions) throws CliArgsException {
		final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
		return CliFrontendParser.parse(commandLineOptions, args, stopAtNonOptions);
	}
```



##### 1.3 使用FlinkYarnSessionCli

##### CliFronted.java

```java
public static void main(final String[] args) {
	...	...
		// TODO by lwq 初始化命令行对象
		final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
			configuration,
			configurationDirectory);
	...	...
}
```

```java
	public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
		List<CustomCommandLine> customCommandLines = new ArrayList<>();
		customCommandLines.add(new ExecutorCLI(configuration));

		//	Command line interface of the YARN session, with a special initialization here
		//	to prefix all options with y/yarn.
		final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
		try {
			customCommandLines.add(
				loadCustomCommandLine(flinkYarnSessionCLI,
					configuration,
					configurationDirectory,
					"y",
					"yarn"));
		} catch (NoClassDefFoundError | Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
		}

		//	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
		//	      active CustomCommandLine in order and DefaultCLI isActive always return true.
		// 在最后添加DefaultCLI命令行
		customCommandLines.add(new DefaultCLI(configuration));

		return customCommandLines;
	}
```

**添加了3个CustomCommandLine，按顺序分别是：ExecutorCLI，FlinkYarnSessionCli和DefaultCLI。**

**后面会按照这个顺序遍历，选出一个可以使用的CustomCommandLine。**

ExecutorCLI：当指定execution.target参数和-e参数时会优先使用

FlinkYarnSessionCli：当使用yarn模式提交时会使用，例如命令行参数中有-m yarn-cluster，yarn-session或yarn-per-job等

DefaultCLI：都没有匹配上时使用这个

获得CustomCommandLine代码如下：

**CliFronted.java**

```java
	protected void run(String[] args) throws Exception {
		...	...
		final Configuration effectiveConfiguration =
			getEffectiveConfiguration(commandLine, programOptions, jobJars);
		...	...
	}
```

```java
	private Configuration getEffectiveConfiguration(
		final CommandLine commandLine,
		final ProgramOptions programOptions,
		final List<URL> jobJars) throws FlinkException {

        ...	...
		// TODO by lwq 获取激活的命令行
		final CustomCommandLine customCommandLine = getActiveCustomCommandLine(checkNotNull(commandLine));
		...	...
	}
```

```java
public CustomCommandLine getActiveCustomCommandLine(CommandLine commandLine) {
   LOG.debug("Custom commandlines: {}", customCommandLines);
   // TODO by lwq 根据加入列表的顺序，获取激活的命令行
   for (CustomCommandLine cli : customCommandLines) {
      LOG.debug("Checking custom commandline {}, isActive: {}", cli, cli.isActive(commandLine));
      if (cli.isActive(commandLine)) {
         return cli;
      }
   }
   throw new IllegalStateException("No command-line ran.");
}
```



##### 1.4 执行用户代码

**CliFronted.java**

```java
protected void run(String[] args) throws Exception {
	...	...
	executeProgram(effectiveConfiguration, program);
	...	...
}
```

```java
protected void executeProgram(final Configuration configuration, final PackagedProgram program) throws ProgramInvocationException {
	ClientUtils.executeProgram(DefaultExecutorServiceLoader.INSTANCE, configuration, program);
}
```

```java
public static void executeProgram(
			PipelineExecutorServiceLoader executorServiceLoader,
			Configuration configuration,
			PackagedProgram program) throws ProgramInvocationException {
			...	...
			program.invokeInteractiveModeForExecution();
			...	..
}
```

```java
	public void invokeInteractiveModeForExecution() throws ProgramInvocationException {
		callMainMethod(mainClass, args);
	}
```

```java
private static void callMainMethod(Class<?> entryClass, String[] args) throws ProgramInvocationException {
	...	...
	mainMethod.invoke(null, (Object) args);
	...	...
}
```



##### 1.5 生成StreamGraph

第4步就开始执行用户代码的main方法了，main方法中会根据使用的算子，将算子转化为Transformation对象，然后添加到StreamExecutionEnvironment的transformations列表中，这个列表在生成StreamGraph时会用到。

先看一下是如何添加到transformations列表中，

我们以flatMap这个算子为例：

**StreamExecutionEnvironment.java**

```java
public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {

   TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes(clean(flatMapper),
         getType(), Utils.getCallLocationName(), true);

   return flatMap(flatMapper, outType);
}
```

```java
public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper, TypeInformation<R> outputType) {
	return transform("Flat Map", outputType, new StreamFlatMap<>(clean(flatMapper)));
}
```

```java
public <R> SingleOutputStreamOperator<R> transform(
		String operatorName,
		TypeInformation<R> outTypeInfo,
		OneInputStreamOperator<T, R> operator) {
	// TODO by lwq
	return doTransform(operatorName, outTypeInfo, SimpleOperatorFactory.of(operator));
}
```

```java
protected <R> SingleOutputStreamOperator<R> doTransform(
		String operatorName,
		TypeInformation<R> outTypeInfo,
		StreamOperatorFactory<R> operatorFactory) {
	// read the output type of the input Transform to coax out errors about MissingTypeInfo
	transformation.getOutputType();
	// TODO by lwq 创建transformation
	OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
			this.transformation,
			operatorName,
			operatorFactory,
			outTypeInfo,
			environment.getParallelism());
	@SuppressWarnings({"unchecked", "rawtypes"})
	SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, resultTransform);
	// TODO by lwq 将transformation添加到env的transformations集合
	getExecutionEnvironment().addOperator(resultTransform);
	return returnStream;
}
```

```java
public void addOperator(Transformation<?> transformation) {
	...	...
	this.transformations.add(transformation);
}
```



执行完用户代码后，会调用StreamExecutionEnvironment的execute方法来执行任务。

在这里会生成StreamGraph，代码如下：

**StreamExecutionEnvironment.java**

```java
public JobExecutionResult execute(String jobName) throws Exception {
   Preconditions.checkNotNull(jobName, "Streaming Job name should not be null.");

   // TODO by lwq 先创建StreamGraph，再执行
   return execute(getStreamGraph(jobName));
}
```

```java
public StreamGraph getStreamGraph(String jobName) {
	// TODO by lwq 创建StreamGraph
	return getStreamGraph(jobName, true);
}
```

```java
public StreamGraph getStreamGraph(String jobName, boolean clearTransformations) {
	// TODO by lwq 通过调用StreamGraphGenerator的generate方法来生成StreamGraph
	StreamGraph streamGraph = getStreamGraphGenerator().setJobName(jobName).generate();
	if (clearTransformations) {
		this.transformations.clear();
	}
	return streamGraph;
}
```

```java
public StreamGraph generate() {
	// TODO by lwq 生成streamGraph
	streamGraph = new StreamGraph(executionConfig, checkpointConfig, savepointRestoreSettings);
	streamGraph.setStateBackend(stateBackend);
	streamGraph.setChaining(chaining);
	streamGraph.setScheduleMode(scheduleMode);
	streamGraph.setUserArtifacts(userArtifacts);
	streamGraph.setTimeCharacteristic(timeCharacteristic);
	streamGraph.setJobName(jobName);
	streamGraph.setBlockingConnectionsBetweenChains(blockingConnectionsBetweenChains);
	alreadyTransformed = new HashMap<>();
	// TODO by lwq 主要代码
	// TODO by lwq 遍历transformations中的算子进行转换
	for (Transformation<?> transformation: transformations) {
		transform(transformation);
	}
	final StreamGraph builtStreamGraph = streamGraph;
	alreadyTransformed.clear();
	alreadyTransformed = null;
	streamGraph = null;
	return builtStreamGraph;
}
```



##### 1.6 创建JobGraph

**StreamExectutionEnvironment.java**

```java
public JobExecutionResult execute(String jobName) throws Exception {
	Preconditions.checkNotNull(jobName, "Streaming Job name should not be null.");
	// TODO by lwq 先创建StreamGraph，再执行
	return execute(getStreamGraph(jobName));
}
```

```java
public JobExecutionResult execute(StreamGraph streamGraph) 
   // TODO by lwq 执行方法
   final JobClient jobClient = executeAsync(streamGraph);
  ...	...
}
```

```java
public JobClient executeAsync(StreamGraph streamGraph) throws Exception 
	...	...
	CompletableFuture<? extends JobClient> jobClientFuture = executorFactory
	.getExecutor(configuration)
	.execute(streamGraph, configuration);
	...	...
}
```

这里executorFactory的实现是YarnJobClusterExecutorFactory，getExecutor方法返回YarnJobClusterExecutor，然后调用父类AbstractJobClusterExecutor的execute方法。

**YarnJobClusterExecutor.java**

```java
public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception {
	// TODO by lwq 生成JobGraph
	final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);
	...	...
}
```



##### 1.7 上传jar包和配置

**YarnJobClusterExecutor.java**

```java
public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception {
	
	// TODO by lwq 如果是yarn模式，clusterClientFactory为YarnClusterClientFactory
    // 返回的clusterDescriptor为YarnClusterDescriptor
	try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
		final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
        // 调用AbstractContainerizedClusterClientFactory类的getClusterSpecification方法
		final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(configuration);
		// TODO by lwq 这里面会进行上传jar包和配置的操作
		final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor
				.deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode());
		LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());
		return CompletableFuture.completedFuture(
				new ClusterClientJobClientAdapter<>(clusterClientProvider, jobGraph.getJobID()));
	}
}
```

**YarnClusterDescriptor.java**

```java
public ClusterClientProvider<ApplicationId> deployJobCluster(
	ClusterSpecification clusterSpecification,
	JobGraph jobGraph,
	boolean detached) throws ClusterDeploymentException {
	try {
		// TODO by lwq 部署应用
		return deployInternal(
			clusterSpecification,
			"Flink per-job cluster",
			getYarnJobClusterEntrypoint(),
			jobGraph,
			detached);
	} catch (Exception e) {
		throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", e);
	}
}
```

```java
private ClusterClientProvider<ApplicationId> deployInternal(
		ClusterSpecification clusterSpecification,
		String applicationName,
		String yarnClusterEntrypoint,
		@Nullable JobGraph jobGraph,
		boolean detached) throws Exception {
	...	...
	// TODO by lwq 启动applicationMaster
	ApplicationReport report = startAppMaster(
			flinkConfiguration,
			applicationName,
			yarnClusterEntrypoint,
			jobGraph,
			yarnClient,
			yarnApplication,
			validClusterSpecification);
	...	...
}
```

```java
private ApplicationReport startAppMaster(
		Configuration configuration,
		String applicationName,
		String yarnClusterEntrypoint,
		JobGraph jobGraph,
		YarnClient yarnClient,
		YarnClientApplication yarnApplication,
		ClusterSpecification clusterSpecification) throws Exception {
	// ------------------ Initialize the file systems ------------------------
	org.apache.flink.core.fs.FileSystem.initialize(
			configuration,
			PluginUtils.createPluginManagerFromRootFolder(configuration));
	// 初始化文件系统
	final FileSystem fs = FileSystem.get(yarnConfiguration);
	...	...
	// TODO 创建ApplicationSubmissionContext 提交应用的上下文
	ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
	// The files need to be shipped and added to classpath.
    // 会被添加到classpath中需要上传的文件
	Set<File> systemShipFiles = new HashSet<>(shipFiles.size());
	// The files only need to be shipped.
    // 只上传不添加到classpath
	Set<File> shipOnlyFiles = new HashSet<>();
	for (File file : shipFiles) {
		systemShipFiles.add(file.getAbsoluteFile());
	}
	final String logConfigFilePath = configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
	if (logConfigFilePath != null) {
		systemShipFiles.add(new File(logConfigFilePath));
	}
	addLibFoldersToShipFiles(systemShipFiles);
	// Plugin files only need to be shipped and should not be added to classpath.
	addPluginsFoldersToShipFiles(shipOnlyFiles);
	// Set-up ApplicationSubmissionContext for the application
	final ApplicationId appId = appContext.getApplicationId();
	// ------------------ Add Zookeeper namespace to local flinkConfiguraton ------
	...	...
	configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);
	// yarn的重试次数
    if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
		// activate re-execution of failed applications
		appContext.setMaxAppAttempts(
				configuration.getInteger(
						YarnConfigOptions.APPLICATION_ATTEMPTS.key(),
						YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));
		activateHighAvailabilitySupport(appContext);
	} else {
		// set number of application retries to 1 in the default case
		appContext.setMaxAppAttempts(
				configuration.getInteger(
						YarnConfigOptions.APPLICATION_ATTEMPTS.key(),
						1));
	}
	final Set<File> userJarFiles = (jobGraph == null)
			// not per-job submission
			? Collections.emptySet()
			// add user code jars from the provided JobGraph
			: jobGraph.getUserJars().stream().map(f -> f.toUri()).map(File::new).collect(Collectors.toSet());
	// only for per job mode
	if (jobGraph != null) {
		for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry : jobGraph.getUserArtifacts().entrySet()) {
			org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(entry.getValue().filePath);
			// only upload local files
			if (!path.getFileSystem().isDistributedFS()) {
				Path localPath = new Path(path.getPath());
				Tuple2<Path, Long> remoteFileInfo =
					Utils.uploadLocalFileToRemote(fs, appId.toString(), localPath, homeDir, entry.getKey());
				jobGraph.setUserArtifactRemotePath(entry.getKey(), remoteFileInfo.f0.toString());
			}
		}
		jobGraph.writeUserArtifactEntriesToConfiguration();
	}
	// local resource map for Yarn
	final Map<String, LocalResource> localResources = new HashMap<>(2 + systemShipFiles.size() + userJarFiles.size());
	// list of remote paths (after upload)
	final List<Path> paths = new ArrayList<>(2 + systemShipFiles.size() + userJarFiles.size());
	// ship list that enables reuse of resources for task manager containers
	StringBuilder envShipFileList = new StringBuilder();
	// upload and register ship files, these files will be added to classpath.
    // 上传添加到classpath的文件
	List<String> systemClassPaths = uploadAndRegisterFiles(
		systemShipFiles,
		fs,
		homeDir,
		appId,
		paths,
		localResources,
		Path.CUR_DIR,
		envShipFileList);
	// upload and register ship-only files
    // 上传不需要添加到classapth的文件
	uploadAndRegisterFiles(
		shipOnlyFiles,
		fs,
		homeDir,
		appId,
		paths,
		localResources,
		Path.CUR_DIR,
		envShipFileList);
    // 上传用户代码
	final List<String> userClassPaths = uploadAndRegisterFiles(
		userJarFiles,
		fs,
		homeDir,
		appId,
		paths,
		localResources,
		userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ?
			ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR,
		envShipFileList);
	...	...
    
}
```



##### 1.8 封装提交参数和命令

```java
private ApplicationReport startAppMaster(
		Configuration configuration,
		String applicationName,
		String yarnClusterEntrypoint,
		JobGraph jobGraph,
		YarnClient yarnClient,
		YarnClientApplication yarnApplication,
		ClusterSpecification clusterSpecification) throws Exception {
	...	...
	// 封装启动AM container的java 命令
	final ContainerLaunchContext amContainer = setupApplicationMasterContainer(
			yarnClusterEntrypoint,
			hasLogback,
			hasLog4j,
			hasKrb5,
			clusterSpecification.getMasterMemoryMB());
	
    ...	...
	amContainer.setLocalResources(localResources);
    ...	...
	// Setup CLASSPATH and environment variables for ApplicationMaster
	final Map<String, String> appMasterEnv = new HashMap<>();
	// set user specified app master environment variables
	appMasterEnv.putAll(
		BootstrapTools.getEnvironmentVariables(ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX, configuration));
	// set Flink app class path
	appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
	// set Flink on YARN internal configuration values
	appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, remotePathJar.toString());
	appMasterEnv.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
	appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, homeDir.toString());
	appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, envShipFileList.toString());
	appMasterEnv.put(YarnConfigKeys.ENV_ZOOKEEPER_NAMESPACE, getZookeeperNamespace());
	appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES, yarnFilesDir.toUri().toString());
	// https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
	appMasterEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
	...	...
	//To support Yarn Secure Integration Test Scenario
	if (remoteYarnSiteXmlPath != null) {
		appMasterEnv.put(YarnConfigKeys.ENV_YARN_SITE_XML_PATH, remoteYarnSiteXmlPath.toString());
	}
	...	...
	Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);
	amContainer.setEnvironment(appMasterEnv);
	// Set up resource type requirements for ApplicationMaster
	Resource capability = Records.newRecord(Resource.class);
	capability.setMemory(clusterSpecification.getMasterMemoryMB());
	capability.setVirtualCores(flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES));
	final String customApplicationName = customName != null ? customName : applicationName;
	appContext.setApplicationName(customApplicationName);
	appContext.setApplicationType(applicationType != null ? applicationType : "Apache Flink");
	appContext.setAMContainerSpec(amContainer);
	appContext.setResource(capability);
	...	...
	// TODO by lwq 通过yarnClient提交应用
	LOG.info("Submitting application master " + appId);
	yarnClient.submitApplication(appContext);
	...	...
}
```



##### 1.9 提交应用

**YarnClientImpl.java**

```java
public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    ApplicationId applicationId = appContext.getApplicationId();
    ...	...
    SubmitApplicationRequest request =
        Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);

    ...	...

    //TODO: YARN-1763:Handle RM failovers during the submitApplication call.
    // 
    rmClient.submitApplication(request);
	...	...
}
```

**ApplicationClientProtocolPBClientImpl.java**

```java
public SubmitApplicationResponse submitApplication(
    SubmitApplicationRequest request) throws YarnException,
    IOException {
  //取出报文
  SubmitApplicationRequestProto requestProto =
      ((SubmitApplicationRequestPBImpl) request).getProto();
	...	...
    //将报文发送发送到服务端，并将返回结果构成 response
    return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,
      requestProto));
  	...	...
}
```

**ApplicationClientProtocolPBServiceImpl.java**

```java
public SubmitApplicationResponseProto submitApplication(RpcController arg0,
    SubmitApplicationRequestProto proto) throws ServiceException {
  SubmitApplicationRequestPBImpl request = new SubmitApplicationRequestPBImpl(proto);
  	... ...
    // 通过RPC调用submitApplication方法
    SubmitApplicationResponse response = real.submitApplication(request);
    return ((SubmitApplicationResponsePBImpl)response).getProto();
	... ...
}
```

```java
public SubmitApplicationResponse submitApplication(
    SubmitApplicationRequest request) throws YarnException {
  ApplicationSubmissionContext submissionContext = request
      .getApplicationSubmissionContext();
  ApplicationId applicationId = submissionContext.getApplicationId();
    ...	...
    // 调用RMAppManager的submitApplication方法处理提交的应用
    // call RMAppManager to submit application directly
    rmAppManager.submitApplication(submissionContext,
        System.currentTimeMillis(), user);
  ...	...
}
```



#### 2.启动ApplicationMaster

Per-job 模式的 AM container 加载运行入口是 YarnJobClusterEntryPoint 中的 main()方法
**YarnJobClusterEntrypoint.java**  

```java
public static void main(String[] args) {
	// startup checks and logging
	// TODO by lwq 检查并且打印环境信息日志
	EnvironmentInformation.logEnvironmentInfo(LOG, YarnJobClusterEntrypoint.class.getSimpleName(), args);
	SignalHandler.register(LOG);
	JvmShutdownSafeguard.installAsShutdownHook(LOG);
	Map<String, String> env = System.getenv();
	final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
	Preconditions.checkArgument(
		workingDirectory != null,
		"Working directory variable (%s) not set",
		ApplicationConstants.Environment.PWD.key());
	// TODO by lwq 打印yarn相关的日志
	try {
		YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
	} catch (IOException e) {
		LOG.warn("Could not log YARN environment information.", e);
	}
	Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env);
	YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(
		configuration);
	// TODO by lwq 入口类
	ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
}
```



#### 3.AM启动内部组件

##### 3.1 启动Dispatcher

**ClusterEntryPoint.java**

```java
public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {
	final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
	// TODO by lwq 启动集群方法
	...	..
		clusterEntrypoint.startCluster();
	...	..
}
```

```java
public void startCluster() throws ClusterEntrypointException {
	...	...
		// TODO by lwq 运行集群方法
		securityContext.runSecured((Callable<Void>) () -> {
			runCluster(configuration);
			return null;
		});
	...	...
}
```

```java
private void runCluster(Configuration configuration) throws Exception {
	synchronized (lock) {
		// TODO by lwq 第一步
		// TODO by lwq 初始化各种服务
        // 其中有从本地创建的JobGraph
		initializeServices(configuration);
		...	...
		// TODO by lwq 第二步
		// TODO by lwq 负责初始化了很多组件的工厂实例
		// TODO by lwq 如果是yarn的per-job模式，各种工厂实现的实现如下：
		// TODO by lwq DispatcherRunnerFactory，默认实现：DefaultDispatcherRunnerFactory
		// TODO by lwq ResourceManagerFactory，默认实现：YarnResourceManagerFactory
		// TODO by lwq RestEndpointFactory， 默认实现：JobRestEndpointFactory
		final DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory = createDispatcherResourceManagerComponentFactory(configuration);
		// TODO by lwq 第三步
		// TODO by lwq 主要创建了三个重要的组件
		// TODO by lwq DispatcherRunner，ResourceManager，
        // TODO by lwq 并且启动上面三个组件
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

**DefaultDispatcherResourceManagerComponentFactory.java**

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
		...	...
		log.debug("Starting Dispatcher.");
		// TODO by lwq 重要组件3
		// TODO by lwq 重要组件Dispatcher，实现是：DefaultDispatcherRunner
		dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(
			highAvailabilityServices.getDispatcherLeaderElectionService(),
			fatalErrorHandler,
			new HaServicesJobGraphStoreFactory(highAvailabilityServices),
			ioExecutor,
			rpcService,
			partialDispatcherServices);
		...	..
}
```

**DefaultDispatcherRunnerFactory.java**

```java
public DispatcherRunner createDispatcherRunner(
		LeaderElectionService leaderElectionService,
		FatalErrorHandler fatalErrorHandler,
		JobGraphStoreFactory jobGraphStoreFactory,
		Executor ioExecutor,
		RpcService rpcService,
		PartialDispatcherServices partialDispatcherServices) throws Exception {
	final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactoryFactory.createFactory(
		jobGraphStoreFactory,
		ioExecutor,
		rpcService,
		partialDispatcherServices,
		fatalErrorHandler);
	return DefaultDispatcherRunner.create(
		leaderElectionService,
		fatalErrorHandler,
		dispatcherLeaderProcessFactory);
}
```

**DefaultDispatcherRunner.java**

```java
public static DispatcherRunner create(
		LeaderElectionService leaderElectionService,
		FatalErrorHandler fatalErrorHandler,
		DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory) throws Exception {
	final DefaultDispatcherRunner dispatcherRunner = new DefaultDispatcherRunner(
		leaderElectionService,
		fatalErrorHandler,
		dispatcherLeaderProcessFactory);
	return DispatcherRunnerLeaderElectionLifecycleManager.createFor(dispatcherRunner, leaderElectionService);
}
```

**DispatcherRunnerLeaderElectionLifecycleManager.java**

```java
public static <T extends DispatcherRunner & LeaderContender> DispatcherRunner createFor(T dispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
	return new DispatcherRunnerLeaderElectionLifecycleManager<>(dispatcherRunner, leaderElectionService);
```

**StandaloneLeaderElectionService.java**

```java
public void start(LeaderContender newContender) throws Exception {
	if (contender != null) {
		// Service was already started
		throw new IllegalArgumentException("Leader election service cannot be started multiple times.");
	}
	contender = Preconditions.checkNotNull(newContender);
	// directly grant leadership to the given contender
	contender.grantLeadership(HighAvailabilityServices.DEFAULT_LEADER_ID);
}
```

**DefaultDispatcherRunner.java**

```java
public void grantLeadership(UUID leaderSessionID) {
	runActionIfRunning(() -> startNewDispatcherLeaderProcess(leaderSessionID));
}
```

```java
private void startNewDispatcherLeaderProcess(UUID leaderSessionID) {
	stopDispatcherLeaderProcess();
	dispatcherLeaderProcess = createNewDispatcherLeaderProcess(leaderSessionID);
	final DispatcherLeaderProcess newDispatcherLeaderProcess = dispatcherLeaderProcess;
    // 执行start方法
	FutureUtils.assertNoException(
		previousDispatcherLeaderProcessTerminationFuture.thenRun(newDispatcherLeaderProcess::start));
}
```

**AbstractDispatcherLeaderProcess.java**

```java
public final void start() {
	runIfStateIs(
		State.CREATED,
		this::startInternal);
}
```

```java
private void startInternal() {
	log.info("Start {}.", getClass().getSimpleName());
	state = State.RUNNING;
	onStart();
}
```

**JobDispatcherLeaderProcess.java**

```java
protected void onStart() {
	final DispatcherGatewayService dispatcherService = dispatcherGatewayServiceFactory.create(
		DispatcherId.fromUuid(getLeaderSessionId()),
		Collections.singleton(jobGraph),
		ThrowingJobGraphWriter.INSTANCE);
	completeDispatcherSetup(dispatcherService);
}
```

**DefaultDispatcherGatewayServiceFactory.java**

```java
public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
		DispatcherId fencingToken,
		Collection<JobGraph> recoveredJobs,
		JobGraphWriter jobGraphWriter) {
	final Dispatcher dispatcher;
	try {
		dispatcher = dispatcherFactory.createDispatcher(
			rpcService,
			fencingToken,
			recoveredJobs,
			PartialDispatcherServicesWithJobGraphStore.from(partialDispatcherServices, jobGraphWriter));
	} catch (Exception e) {
		throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
	}
	dispatcher.start();
	return DefaultDispatcherGatewayService.from(dispatcher);
}
```

**RpcEndpoint.java**

```java
public final void start() {
	rpcServer.start();
}
```

后面就是Rpc的启动，最后会调用到RpcEndpoint的onStart方法，因为这个RpcEndpoint的实现是Dispatcher，所以执行的就是Dispatcher的onStart方法。

**Dispatcher.java**

```java
public void onStart() throws Exception {
	try {
		startDispatcherServices();
	} catch (Exception e) {
		final DispatcherException exception = new DispatcherException(String.format("Could not start the Dispatcher %s", getAddress()), e);
		onFatalError(exception);
		throw exception;
	}
    // 在这里面会启动JobMaster
	startRecoveredJobs();
}
```



##### 3.2 启动ResourceManager

```java
@Override
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
	...	...
	ResourceManager<?> resourceManager = null;
	...	...
	
		// TODO by lwq 重要组件2
		// TODO by lwq 如果是yarn模式，重要组件ResourceManager，实现是YarnResourceManager
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
		...	...
        // 启动resourceManager
		resourceManager.start();
		resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
		...	...
}
```

**RpcEndpoint.java**

```java
public final void start() {
	rpcServer.start();
}
```

**执行ResourceManager.java的onStart方法**

```java
@Override
public void onStart() throws Exception {
	try {
		startResourceManagerServices();
	} catch (Exception e) {
		final ResourceManagerException exception = new ResourceManagerException(String.format("Could not start the ResourceManager %s", getAddress()), e);
		onFatalError(exception);
		throw exception;
	}
}
```

```java
private void startResourceManagerServices() throws Exception {
	try {
		leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();
		initialize();
		leaderElectionService.start(this);
		jobLeaderIdService.start(new JobLeaderIdActionsImpl());
		registerSlotAndTaskExecutorMetrics();
	} catch (Exception e) {
		handleStartResourceManagerServicesException(e);
	}
}
```

**YarnResourceManager.java**

```java
protected void initialize() throws ResourceManagerException {
	try {
        // 创建yarn的resouceManager客户端
		resourceManagerClient = createAndStartResourceManagerClient(
			yarnConfig,
			yarnHeartbeatIntervalMillis,
			webInterfaceUrl);
	} catch (Exception e) {
		throw new ResourceManagerException("Could not start resource manager client.", e);
	}
    // 创建yarn的nodeManager客户端
	nodeManagerClient = createAndStartNodeManagerClient(yarnConfig);
}
```

**StandaloneLeaderElectionService.java**

```java
public void start(LeaderContender newContender) throws Exception {
	if (contender != null) {
		// Service was already started
		throw new IllegalArgumentException("Leader election service cannot be started multiple times.");
	}
	contender = Preconditions.checkNotNull(newContender);
	// directly grant leadership to the given contender
	contender.grantLeadership(HighAvailabilityServices.DEFAULT_LEADER_ID);
}
```

**ResourceManager.java**

```java
public void grantLeadership(final UUID newLeaderSessionID) {
    // 启动slotManager
	final CompletableFuture<Boolean> acceptLeadershipFuture = clearStateFuture
		.thenComposeAsync((ignored) -> tryAcceptLeadership(newLeaderSessionID), getUnfencedMainThreadExecutor());
	final CompletableFuture<Void> confirmationFuture = acceptLeadershipFuture.thenAcceptAsync(
		(acceptLeadership) -> {
			if (acceptLeadership) {
				// confirming the leader session ID might be blocking,
				leaderElectionService.confirmLeadership(newLeaderSessionID, getAddress());
			}
		},
		getRpcService().getExecutor());
	confirmationFuture.whenComplete(
		(Void ignored, Throwable throwable) -> {
			if (throwable != null) {
				onFatalError(ExceptionUtils.stripCompletionException(throwable));
			}
		});
}
```

**ResourceManager.java**

```java
private CompletableFuture<Boolean> tryAcceptLeadership(final UUID newLeaderSessionID) {
		...	...
		startServicesOnLeadership();
		...	...
	} else {
		return CompletableFuture.completedFuture(false);
	}
}
```

```java
protected void startServicesOnLeadership() {
    // 启动心跳服务
	startHeartbeatServices();
    // 启动slotManager
	slotManager.start(getFencingToken(), getMainThreadExecutor(), new ResourceActionsImpl());
}
```



##### 3.3 启动JobMaster

JobMaster是通过Dispatcher来启动的，我们直接看Dispatcher启动的方法。

**Dispatcher.java**

```java
public void onStart() throws Exception {
	try {
		startDispatcherServices();
	} catch (Exception e) {
		final DispatcherException exception = new DispatcherException(String.format("Could not start the Dispatcher %s", getAddress()), e);
		onFatalError(exception);
		throw exception;
	}
    // 在这里面会启动JobMaster
	startRecoveredJobs();
}
```

```java
private void startRecoveredJobs() {
    // 运行jobGraph
	for (JobGraph recoveredJob : recoveredJobs) {
		FutureUtils.assertNoException(runJob(recoveredJob)
			.handle(handleRecoveredJobStartError(recoveredJob.getJobID())));
	}
	recoveredJobs.clear();
}
```

```java
private CompletableFuture<Void> runJob(JobGraph jobGraph) {
	// 在这里会创建JobMaster
	final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);
	jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);
    // 启动JobManagerRunner
	return jobManagerRunnerFuture
		.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
		.thenApply(FunctionUtils.nullFn())
		.whenCompleteAsync(
			(ignored, throwable) -> {
				if (throwable != null) {
					jobManagerRunnerFutures.remove(jobGraph.getJobID());
				}
			},
			getMainThreadExecutor());
}
```

```java
private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
	...	...
    // 启动JobManagerRunner
	jobManagerRunner.start();
	return jobManagerRunner;
}
```

**JobManagerRunnerImpl.java**

```java
public void start() throws Exception {
	try {
		leaderElectionService.start(this);
	} catch (Exception e) {
		log.error("Could not start the JobManager because the leader election service did not start.", e);
		throw new Exception("Could not start the leader election service.", e);
	}
}
```

**StandaloneLeaderElectionService.java**

```java
public void start(LeaderContender newContender) throws Exception {
	if (contender != null) {
		// Service was already started
		throw new IllegalArgumentException("Leader election service cannot be started multiple times.");
	}
	contender = Preconditions.checkNotNull(newContender);
	// directly grant leadership to the given contender
	contender.grantLeadership(HighAvailabilityServices.DEFAULT_LEADER_ID);
}
```

**JobManagerRunnerImpl.java**

```java
public void grantLeadership(final UUID leaderSessionID) {
	synchronized (lock) {
            if (shutdown) {
                log.info("JobManagerRunner already shutdown.");
			return;
		}
		leadershipOperation = leadershipOperation.thenCompose(
			(ignored) -> {
				synchronized (lock) {
                    // 启动
					return verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);
				}
			});
		handleException(leadershipOperation, "Could not start the job manager.");
	}
}
```

```java
private CompletableFuture<Void> verifyJobSchedulingStatusAndStartJobManager(UUID leaderSessionId) {
	final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();
	return jobSchedulingStatusFuture.thenCompose(
		jobSchedulingStatus -> {
			if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
				return jobAlreadyDone();
			} else {
				return startJobMaster(leaderSessionId);
			}
		});
}
```

```java
private CompletionStage<Void> startJobMaster(UUID leaderSessionId) {
	log.info("JobManager runner for job {} ({}) was granted leadership with session id {} at {}.",
	...	...
	final CompletableFuture<Acknowledge> startFuture;
	try {
		startFuture = jobMasterService.start(new JobMasterId(leaderSessionId));
	} catch (Exception e) {
		return FutureUtils.completedExceptionally(new FlinkException("Failed to start the JobMaster.", e));
	}
	final CompletableFuture<JobMasterGateway> currentLeaderGatewayFuture = leaderGatewayFuture;
	return startFuture.thenAcceptAsync(
		(Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader(
			leaderSessionId,
			jobMasterService.getAddress(),
			currentLeaderGatewayFuture),
		executor);
}
```

**JobMaster.java**

```java
public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
	// make sure we receive RPC and async calls
	start();
	return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
}
```

```java
private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {
	...	...
    // 启动JobMaster的服务
	startJobMasterServices();
	...	..
    // 开始调度
	resetAndStartScheduler();
}
```



##### 3.4 生成ExecutionGraph

**生成ExecutionGraph是在创建JobMaster的时候，这个顺序应该是在启动之前，这里没有按照代码顺序。**

**Dispatcher.java**

```java
private CompletableFuture<Void> runJob(JobGraph jobGraph) {
	// 在这里会创建JobMaster
	final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);
	...	...
}
```

```java
private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph) {
	final RpcService rpcService = getRpcService();
    // createJobManagerRunner
	return CompletableFuture.supplyAsync(
		CheckedSupplier.unchecked(() ->
			jobManagerRunnerFactory.createJobManagerRunner(
				jobGraph,
				configuration,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				jobManagerSharedServices,
				new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
				fatalErrorHandler)),
		rpcService.getExecutor());
}
```

**DefaultJobManagerRunnerFactory.java**

```java
public JobManagerRunner createJobManagerRunner(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		JobManagerSharedServices jobManagerServices,
		JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
		FatalErrorHandler fatalErrorHandler) throws Exception {
	...	...
    // 创建JobManagerRunnerImpl
	return new JobManagerRunnerImpl(
		jobGraph,
		jobMasterFactory,
		highAvailabilityServices,
		jobManagerServices.getLibraryCacheManager(),
		jobManagerServices.getScheduledExecutorService(),
		fatalErrorHandler);
}
```

**JobManagerRunnerImpl.java**

```java
public JobManagerRunnerImpl(
		final JobGraph jobGraph,
		final JobMasterServiceFactory jobMasterFactory,
		final HighAvailabilityServices haServices,
		final LibraryCacheManager libraryCacheManager,
		final Executor executor,
		final FatalErrorHandler fatalErrorHandler) throws Exception {
	...	...
        // 创建JobMasterService
		this.jobMasterService = jobMasterFactory.createJobMasterService(jobGraph, this, userCodeLoader);
	... ...
}
```

**DefaultJobMasterServiceFactory.java**

```java
public JobMaster createJobMasterService(
		JobGraph jobGraph,
		OnCompletionActions jobCompletionActions,
		ClassLoader userCodeClassloader) throws Exception {
	return new JobMaster(
		rpcService,
		jobMasterConfiguration,
		ResourceID.generate(),
		jobGraph,
		haServices,
		slotPoolFactory,
		schedulerFactory,
		jobManagerSharedServices,
		heartbeatServices,
		jobManagerJobMetricGroupFactory,
		jobCompletionActions,
		fatalErrorHandler,
		userCodeClassloader,
		schedulerNGFactory,
		shuffleMaster,
		lookup -> new JobMasterPartitionTrackerImpl(
			jobGraph.getJobID(),
			shuffleMaster,
			lookup
		));
}
```

**JobMaster.java**

```java
public JobMaster(
		RpcService rpcService,
		JobMasterConfiguration jobMasterConfiguration,
		ResourceID resourceId,
		JobGraph jobGraph,
		HighAvailabilityServices highAvailabilityService,
		SlotPoolFactory slotPoolFactory,
		SchedulerFactory schedulerFactory,
		JobManagerSharedServices jobManagerSharedServices,
		HeartbeatServices heartbeatServices,
		JobManagerJobMetricGroupFactory jobMetricGroupFactory,
		OnCompletionActions jobCompletionActions,
		FatalErrorHandler fatalErrorHandler,
		ClassLoader userCodeLoader,
		SchedulerNGFactory schedulerNGFactory,
		ShuffleMaster<?> shuffleMaster,
		PartitionTrackerFactory partitionTrackerFactory) throws Exception {
	// 创建slotPool
	this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jobGraph.getJobID());
	// 创建SchedulerNG
	this.schedulerNG = createScheduler(jobManagerJobMetricGroup);
	
}
```

```java
private SchedulerNG createScheduler(final JobManagerJobMetricGroup jobManagerJobMetricGroup) throws Exception {
    // 工厂类创建SchedulerNG
	return schedulerNGFactory.createInstance(
		log,
		jobGraph,
		backPressureStatsTracker,
		scheduledExecutorService,
		jobMasterConfiguration.getConfiguration(),
		scheduler,
		scheduledExecutorService,
		userCodeLoader,
		highAvailabilityServices.getCheckpointRecoveryFactory(),
		rpcTimeout,
		blobWriter,
		jobManagerJobMetricGroup,
		jobMasterConfiguration.getSlotRequestTimeout(),
		shuffleMaster,
		partitionTracker);
}
```

**DefaultSchedulerFactory.java**

```java
public SchedulerNG createInstance(
		final Logger log,
		final JobGraph jobGraph,
		final BackPressureStatsTracker backPressureStatsTracker,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final SlotProvider slotProvider,
		final ScheduledExecutorService futureExecutor,
		final ClassLoader userCodeLoader,
		final CheckpointRecoveryFactory checkpointRecoveryFactory,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final JobManagerJobMetricGroup jobManagerJobMetricGroup,
		final Time slotRequestTimeout,
		final ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker) throws Exception {
	...	...
    // 生成DefaultScheduler
	return new DefaultScheduler(
		log,
		jobGraph,
		backPressureStatsTracker,
		ioExecutor,
		jobMasterConfiguration,
		slotProvider,
		futureExecutor,
		new ScheduledExecutorServiceAdapter(futureExecutor),
		userCodeLoader,
		checkpointRecoveryFactory,
		rpcTimeout,
		blobWriter,
		jobManagerJobMetricGroup,
		slotRequestTimeout,
		shuffleMaster,
		partitionTracker,
		schedulingStrategyFactory,
		FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(jobMasterConfiguration),
		restartBackoffTimeStrategy,
		new DefaultExecutionVertexOperations(),
		new ExecutionVertexVersioner(),
		new DefaultExecutionSlotAllocatorFactory(slotProviderStrategy));
}
```

**DefaultScheduler.java**

```java
public DefaultScheduler(
	...	...
	final JobGraph jobGraph,
	...	...) throws Exception {
    // 创建ExecutionGraph在父类的构造方法里
	super(
		log,
		jobGraph,
		backPressureStatsTracker,
		ioExecutor,
		jobMasterConfiguration,
		slotProvider,
		futureExecutor,
		userCodeLoader,
		checkpointRecoveryFactory,
		rpcTimeout,
		new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
		blobWriter,
		jobManagerJobMetricGroup,
		slotRequestTimeout,
		shuffleMaster,
		partitionTracker,
		executionVertexVersioner,
		false);
	...	...
}
```

**SchedulerBase.java**

```java
public SchedulerBase(
	...	...
	final JobGraph jobGraph,
	...	...) throws Exception {
	...	...
	this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup, checkNotNull(shuffleMaster), checkNotNull(partitionTracker));
	...	...
}
```



#### 4.JobMaster注册，并请求slot

**JobMaster.java**

```java
private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {
	...	...
    // 启动JobMaster的服务
	startJobMasterServices();
	...	..
    // 开始调度
	resetAndStartScheduler();
}
```

```java
private void startJobMasterServices() throws Exception {
    // 启动心跳服务
	startHeartbeatServices();
	// start the slot pool make sure the slot pool now accepts messages for this leader
    // 启动slotPool
	slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
	scheduler.start(getMainThreadExecutor());
	//TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
	// try to reconnect to previously known leader
    // 连接resourceManager
	reconnectToResourceManager(new FlinkException("Starting JobMaster component."));
	// 启动后slotPool开始向slotManager申请slot
	resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
}
```

**StandaloneLeaderRetrievalService.java**

```java
public void start(LeaderRetrievalListener listener) {
	checkNotNull(listener, "Listener must not be null.");
	synchronized (startStopLock) {
		checkState(!started, "StandaloneLeaderRetrievalService can only be started once.");
		started = true;
		// directly notify the listener, because we already know the leading JobManager's address
		listener.notifyLeaderAddress(leaderAddress, leaderId);
	}
}
```

**JobMaster.java**

```java
private class ResourceManagerLeaderListener implements LeaderRetrievalListener {
	@Override
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
		runAsync(
			() -> notifyOfNewResourceManagerLeader(
				leaderAddress,
				ResourceManagerId.fromUuidOrNull(leaderSessionID)));
	}
	@Override
	public void handleError(final Exception exception) {
		handleJobMasterError(new Exception("Fatal error in the ResourceManager leader service", exception));
	}
}
```

```java
private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
	resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
	reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
}
```

```java
private void reconnectToResourceManager(Exception cause) {
	closeResourceManagerConnection(cause);
	tryConnectToResourceManager();
}
```

```java
private void tryConnectToResourceManager() {
	if (resourceManagerAddress != null) {
		connectToResourceManager();
	}
}
```

```java
private void connectToResourceManager() {
	assert(resourceManagerAddress != null);
	assert(resourceManagerConnection == null);
	assert(establishedResourceManagerConnection == null);
	log.info("Connecting to ResourceManager {}", resourceManagerAddr
	resourceManagerConnection = new ResourceManagerConnection(
		log,
		jobGraph.getJobID(),
		resourceId,
		getAddress(),
		getFencingToken(),
		resourceManagerAddress.getAddress(),
		resourceManagerAddress.getResourceManagerId(),
		scheduledExecutorService);
	resourceManagerConnection.start();
}
```

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
    // 调用resourceManagerConnection的start方法
	resourceManagerConnection.start();
}
```

**RegisteredRpcConnection.java**

```java
public void start() {
	checkState(!closed, "The RPC connection is already closed");
	checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");
	// 创建注册对象createNewRegistration()
    final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
    // 开始注册newRegistration.startRegistration();
	if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
		newRegistration.startRegistration();
	} else {
		// concurrent start operation
		newRegistration.cancel();
	}
}
```

```java
private RetryingRegistration<F, G, S> createNewRegistration() {
    // 生成注册对象，generateRegistration()
	RetryingRegistration<F, G, S> newRegistration = checkNotNull(generateRegistration());
	CompletableFuture<Tuple2<G, S>> future = newRegistration.getFuture();
    // 当注册成功时，异步的回调onRegistrationSuccess(result.f1);
	future.whenCompleteAsync(
		(Tuple2<G, S> result, Throwable failure) -> {
			if (failure != null) {
				... ...
			} else {
                // 成功调用
				targetGateway = result.f0;
				onRegistrationSuccess(result.f1);
			}
		}, executor);
	return newRegistration;
}
```

**ResourceManagerConnection.java**

```java
// 调用generateRegistration()方法
protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
    // new出来一个RetryingRegistration，实现了invokeRegistration方法，在后面注册时会调用
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
            // RPC调用，调用ResourceManager的registerJobManager方法
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

创建完RetryingRegistration对象后，开始调用start方法：

**RegisteredRpcConnection.java**

```java
public void start() {
	checkState(!closed, "The RPC connection is already closed");
	checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");
	// 创建注册对象createNewRegistration()
    final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
    // 开始注册newRegistration.startRegistration();
	if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
		newRegistration.startRegistration();
	} else {
		// concurrent start operation
		newRegistration.cancel();
	}
}
```

**RetryingRegistration.java**

```java
public void startRegistration() {
	...	..
	try {
		...	...
		// upon success, start the registration attempts
        // 开始尝试注册
		CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync(
			(G rpcGateway) -> {
				log.info("Resolved {} address, beginning registration", targetName);
                // 在这里进行注册
				register(rpcGateway, 1, retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis());
			},
			rpcService.getExecutor());
		...	...
	catch (Throwable t) {
		completionFuture.completeExceptionally(t);
		cancel();
	}
}
```

```java
private void register(final G gateway, final int attempt, final long timeoutMillis) {
	...	...
	try {
		log.info("Registration at {} attempt {} (timeout={}ms)", targetName, attempt, timeoutMillis);
        // 调用注册的方法
		CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);
		...	...
	}
	catch (Throwable t) {
		..	...
	}
}
```

调用注册的方法，就是前面创建的注册对象时实现的：

```java
protected CompletableFuture<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
			Time timeout = Time.milliseconds(timeoutMillis);
            // RPC调用，调用ResourceManager的registerJobManager方法
			return gateway.registerJobManager(
				jobMasterId,
				jobManagerResourceID,
				jobManagerRpcAddress,
				jobID,
				timeout);
		}
};
```

Rpc调用ResourceManager中的方法：

**ResourceManager.java**

```java
public CompletableFuture<RegistrationResponse> registerJobManager(
		final JobMasterId jobMasterId,
		final ResourceID jobManagerResourceId,
		final String jobManagerAddress,
		final JobID jobId,
		final Time timeout) {
	...	...
	CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);
	CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
		jobMasterIdFuture,
		(JobMasterGateway jobMasterGateway, JobMasterId leadingJobMasterId) -> {
			if (Objects.equals(leadingJobMasterId, jobMasterId)) {
                // 注册JobMaster
				return registerJobMasterInternal(
					jobMasterGateway,
					jobId,
					jobManagerAddress,
					jobManagerResourceId);
			} else {
				...	...
			}
		},
		getMainThreadExecutor());
	...	..
}
```

后面就是具体注册的逻辑，这里先不看了。

继续下面看请求slot的代码：

返回到**JobMaster.java** 的内部类 **ResourceManagerConnection** 

```java
protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
	runAsync(() -> {
		// filter out outdated connections
		//noinspection ObjectEquality
		if (this == resourceManagerConnection) {
            // 走这里
			establishResourceManagerConnection(success);
		}
	});
}
```

```java
private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
	...	...
		establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
			resourceManagerGateway,
			resourceManagerResourceId);
		slotPool.connectToResourceManager(resourceManagerGateway);
	...	...
}
```

**SlotPoolImpl.java**

```java
public void connectToResourceManager(@Nonnull ResourceManagerGateway resourceManagerGateway) {
	this.resourceManagerGateway = checkNotNull(resourceManagerGateway);
	// work on all slots waiting for this connection
	for (PendingRequest pendingRequest : waitingForResourceManager.values()) {
		requestSlotFromResourceManager(resourceManagerGateway, pendingRequest);
	}
	// all sent off
	waitingForResourceManager.clear();
}
```

```java
private void requestSlotFromResourceManager(
		final ResourceManagerGateway resourceManagerGateway,
		final PendingRequest pendingRequest) {
	...	...
    // RPC调用，请求slot
	CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot(
		jobMasterId,
		new SlotRequest(jobId, allocationId, pendingRequest.getResourceProfile(), jobManagerAddress),
		rpcTimeout);
	...	...
}
```

RPC调用，**ResourceManager.java**

```java
public CompletableFuture<Acknowledge> requestSlot(
		JobMasterId jobMasterId,
		SlotRequest slotRequest,
		final Time timeout) {
	...	...
	slotManager.registerSlotRequest(slotRequest);
	... ...
}
```

**SlotManagerImpl.java**

```java
public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
	...	...
    // 请求slot
	internalRequestSlot(pendingSlotRequest);
	...	...
}
```

```java
private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
	final ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();
	OptionalConsumer.of(findMatchingSlot(resourceProfile))
		.ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))
        // 如果没有匹配的资源，就走下面这段代码
		.ifNotPresent(() -> fulfillPendingSlotRequestWithPendingTaskManagerSlot(pendingSlotRequest));
}
```

```java
private void fulfillPendingSlotRequestWithPendingTaskManagerSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
	ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();
	Optional<PendingTaskManagerSlot> pendingTaskManagerSlotOptional = findFreeMatchingPendingTaskManagerSlot(resourceProfile);
    // 没有合适的资源
	if (!pendingTaskManagerSlotOptional.isPresent()) {
        // 分配资源
		pendingTaskManagerSlotOptional = allocateResource(resourceProfile);
	}
	...	...
}
```



#### 5.ResourceManager申请资源

**SlotManagerImpl.java**

```java
private Optional<PendingTaskManagerSlot> allocateResource(ResourceProfile resourceProfile) throws ResourceManagerException {
   // 请求资源
   final Collection<ResourceProfile> requestedSlots = resourceActions.allocateResource(resourceProfile);
   ...	...
}
```

```java
public Collection<ResourceProfile> allocateResource(ResourceProfile resourceProfile) {
	validateRunsInMainThread();
	return startNewWorker(resourceProfile);
}
```

**YarnResourceManager.java**

```java
public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile) {
	if (!resourceProfilesPerWorker.iterator().next().isMatching(resourceProfile)) {
		return Collections.emptyList();
	}
	requestYarnContainer();
	return resourceProfilesPerWorker;
}
```

```java
private void requestYarnContainer() {
    // 请求启动container
	resourceManagerClient.addContainerRequest(getContainerRequest());
	...	...
	resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
	...	...
}
```



#### 6.启动TaskManager

直接转到taskManager的入口类YarnTaskExecutorRunner。

**YarnTaskExecutorRunner.java**

```java
public static void main(String[] args) {
	EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskExecutor runner", args);
	SignalHandler.register(LOG);
	JvmShutdownSafeguard.installAsShutdownHook(LOG);
	runTaskManagerSecurely(args);
}
```

```java
private static void runTaskManagerSecurely(String[] args) {
	try {
		...	...
		SecurityUtils.getInstalledContext().runSecured((Callable<Void>) () -> {
			TaskManagerRunner.runTaskManager(configuration, new ResourceID(containerId));
			return null;
		});
	}
	catch (Throwable t) {
	...	...
	}
}
```

**TaskManagerRunner.java**

```java
public static void runTaskManager(Configuration configuration, ResourceID resourceId) throws Exception {
	final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, resourceId);
	taskManagerRunner.start();
}
```

```java
public void start() throws Exception {
	taskManager.start();
}
```

```java
public final void start() {
	rpcServer.start();
}
```

#### 7.启动TaskExecutor

通过RPC跳转到TaskExecutor的onStart方法

**TaskExecutor.java**

```java
public void onStart() throws Exception {
	try {
		startTaskExecutorServices();
	} catch (Exception e) {
	...	...
	}
	...	...
}
```

```java
private void startTaskExecutorServices() throws Exception {
	try {
		// start by connecting to the ResourceManager
		resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
		// tell the task slot table who's responsible for the task slot actions
		taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());
		// start the job leader service
		jobLeaderService.start(getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());
		fileCache = new FileCache(taskManagerConfiguration.getTmpDirectories(), blobCacheService.getPermanentBlobService());
	} catch (Exception e) {
		handleStartTaskExecutorServicesException(e);
	}
```

#### 8.注册TaskManager

代码接上面，通过resourceManagerLeaderRetriever连接ResourceManager

**TaskExecutor.java**

```java
private void startTaskExecutorServices() throws Exception {
	try {
		// start by connecting to the ResourceManager
		resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
		...	...
	} catch (Exception e) {
		handleStartTaskExecutorServicesException(e);
	}
}
```

**StandaloneLeaderRetrievalService.java**

```java
public void start(LeaderRetrievalListener listener) {
	checkNotNull(listener, "Listener must not be null.");
	synchronized (startStopLock) {
		checkState(!started, "StandaloneLeaderRetrievalService can only be started once.");
		started = true;
		// directly notify the listener, because we already know the leading JobManager's address
		listener.notifyLeaderAddress(leaderAddress, leaderId);
	}
}
```

LeaderRetrievalListener的实现如下：

**TaskExecutor.java**

```java
private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {
	@Override
    // 关键方法
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
		runAsync(
			() -> notifyOfNewResourceManagerLeader(
				leaderAddress,
				ResourceManagerId.fromUuidOrNull(leaderSessionID)));
	}
	@Override
	public void handleError(Exception exception) {
		onFatalError(exception);
	}
}
```

```java
private void notifyOfNewResourceManagerLeader(String newLeaderAddress, ResourceManagerId newResourceManagerId) {
	resourceManagerAddress = createResourceManagerAddress(newLeaderAddress, newResourceManagerId);
	reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
}
```

```java
private void reconnectToResourceManager(Exception cause) {
	closeResourceManagerConnection(cause);
	startRegistrationTimeout();
	tryConnectToResourceManager();
}
```

```java
private void tryConnectToResourceManager() {
	if (resourceManagerAddress != null) {
		connectToResourceManager();
	}
}
```

```java
private void connectToResourceManager() {
	assert(resourceManagerAddress != null);
	assert(establishedResourceManagerConnection == null);
	assert(resourceManagerConnection == null);
	log.info("Connecting to ResourceManager {}.", resourceManagerAddress);
	final TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
		getAddress(),
		getResourceID(),
		taskManagerLocation.dataPort(),
		hardwareDescription,
		taskManagerConfiguration.getDefaultSlotResourceProfile(),
		taskManagerConfiguration.getTotalResourceProfile()
	);
    // 创建了一个TaskExecutorToResourceManagerConnection
	resourceManagerConnection =
		new TaskExecutorToResourceManagerConnection(
			log,
			getRpcService(),
			taskManagerConfiguration.getRetryingRegistrationConfiguration(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			getMainThreadExecutor(),
			new ResourceManagerRegistrationListener(),
			taskExecutorRegistration);
    // 开始连接ResourceManager
	resourceManagerConnection.start();
}
```

**RegisteredRpcConnection.java**

```java
public void start() {
	checkState(!closed, "The RPC connection is already closed");
	checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");
    // 在这里创建一个注册对象
	final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
	if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
        // 开始注册
		newRegistration.startRegistration();
	} else {
		// concurrent start operation
		newRegistration.cancel();
	}
}
```

```java
private RetryingRegistration<F, G, S> createNewRegistration() {
    // 生成RetryingRegistration对象
	RetryingRegistration<F, G, S> newRegistration = checkNotNull(generateRegistration());
	CompletableFuture<Tuple2<G, S>> future = newRegistration.getFuture();
    // 异步调用，如果注册成功那么就进行回调，比较重要的是成功时的回调方法。
	future.whenCompleteAsync(
		(Tuple2<G, S> result, Throwable failure) -> {
			if (failure != null) {
				if (failure instanceof CancellationException) {
					// we ignore cancellation exceptions because they originate from cancelling
					// the RetryingRegistration
					log.debug("Retrying registration towards {} was cancelled.", targetAddress);
				} else {
					// this future should only ever fail if there is a bug, not if the registration is declined
					onRegistrationFailure(failure);
				}
			} else {
				targetGateway = result.f0;
                // 这里比较重要
				onRegistrationSuccess(result.f1);
			}
		}, executor);
	return newRegistration;
}
```

**TaskExecutorToResourceManagerConnection.java**

```java
protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
   log.info("Successful registration at resource manager {} under registration id {}.",
      getTargetAddress(), success.getRegistrationId());
   registrationListener.onRegistrationSuccess(this, success);
}
```



先看一下生成注册对象的代码：

**TaskExecutorToResourceManagerConnection.java**

```java
protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> generateRegistration() {
    // new一个ResourceManagerRegistration
	return new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
		log,
		rpcService,
		getTargetAddress(),
		getTargetLeaderId(),
		retryingRegistrationConfiguration,
		taskExecutorRegistration);
}
```

创建ResourceManagerRegistration里面比较重要的是，它实现了invokeRegistration方法。

这个方法在后面会调用到。

**ResourceManagerRegistration.java**

```
protected CompletableFuture<RegistrationResponse> invokeRegistration(
      ResourceManagerGateway resourceManager, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {
   Time timeout = Time.milliseconds(timeoutMillis);
   return resourceManager.registerTaskExecutor(
      taskExecutorRegistration,
      timeout);
}
```



生成了注册对象之后，开始进行注册。继续注册的代码：

**RegisteredRpcConnection.java**

```java
public void start() {
	checkState(!closed, "The RPC connection is already closed");
	checkState(!isConnected() && pendingRegistration == null, "The RPC connection is already started");
    // 在这里创建一个注册对象
	final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
	if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
        // 开始注册
		newRegistration.startRegistration();
	} else {
		// concurrent start operation
		newRegistration.cancel();
	}
}
```

**RetryingRegistration.java**

```java
public void startRegistration() {
	...	...
	try {
		...	...
		// upon success, start the registration attempts
		CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync(
			(G rpcGateway) -> {
				log.info("Resolved {} address, beginning registration", targetName);
                // 重要的方法在这里，进行注册
				register(rpcGateway, 1, retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis());
			},
			rpcService.getExecutor());
		...	...
	}
	catch (Throwable t) {
		...	...
	}
}
```

```java
private void register(final G gateway, final int attempt, final long timeoutMillis) {
	...	...
	try {
		log.info("Registration at {} attempt {} (timeout={}ms)", targetName, attempt, timeoutMillis);
        // 调用注册的方法
		CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);
		// if the registration was successful, let the TaskExecutor know
        // 如果注册成功了，通知taskExectutor，执行回调方法
		CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync(
			(RegistrationResponse result) -> {
				if (!isCanceled()) {
					if (result instanceof RegistrationResponse.Success) {
						// registration successful!
						S success = (S) result;
                        // 注册成功的回调
						completionFuture.complete(Tuple2.of(gateway, success));
					}
					else {
						...	...
					}
				}
			},
			rpcService.getExecutor());
		...	...
	}
	catch (Throwable t) {
		...	...
	}
}
```

**TaskExecutorToResourceManagerConnection.java**

```java
protected CompletableFuture<RegistrationResponse> invokeRegistration(
		ResourceManagerGateway resourceManager, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {
	Time timeout = Time.milliseconds(timeoutMillis);
    // 在这里RPC调用进行注册
	return resourceManager.registerTaskExecutor(
		taskExecutorRegistration,
		timeout);
}
```

**ResourceManager.java**

```java
public CompletableFuture<RegistrationResponse> registerTaskExecutor(
		final TaskExecutorRegistration taskExecutorRegistration,
		final Time timeout) {
	...	...
	return taskExecutorGatewayFuture.handleAsync(
		(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
			final ResourceID resourceId = taskExecutorRegistration.getResourceId();
			if (taskExecutorGatewayFuture == taskExecutorGatewayFutures.get(resourceId)) {
				taskExecutorGatewayFutures.remove(resourceId);
				if (throwable != null) {
					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
                    // 注册
					return registerTaskExecutorInternal(taskExecutorGateway, taskExecutorRegistration);
				}
			} else {
				...	...
			}
		},
		getMainThreadExecutor());
```

```java
private RegistrationResponse registerTaskExecutorInternal(
		TaskExecutorGateway taskExecutorGateway,
		TaskExecutorRegistration taskExecutorRegistration) {
	ResourceID taskExecutorResourceId = taskExecutorRegistration.getResourceId();
	...	...
	if (newWorker == null) {
		...	...
	} else {
        // 注册成功
		WorkerRegistration<WorkerType> registration = new WorkerRegistration<>(
			taskExecutorGateway,
			newWorker,
			taskExecutorRegistration.getDataPort(),
			taskExecutorRegistration.getHardwareDescription());
		log.info("Registering TaskManager with ResourceID {} ({}) at ResourceManager", taskExecutorResourceId, taskExecutorAddress);
		taskExecutors.put(taskExecutorResourceId, registration);
		taskManagerHeartbeatManager.monitorTarget(taskExecutorResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) {
				// the ResourceManager will always send heartbeat requests to the
				// TaskManager
			}
			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {
				taskExecutorGateway.heartbeatFromResourceManager(resourceID);
			}
		});
		return new TaskExecutorRegistrationSuccess(
			registration.getInstanceID(),
			resourceId,
			clusterInformation);
	}
}
```

如果TaskManager注册成功，会返回结果，TaskManager收到结果后会执行注册成功后的回调方法。

回调方法如下：

**TaskExecutorToResourceManagerConnection.java**

```java
protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
   log.info("Successful registration at resource manager {} under registration id {}.",
      getTargetAddress(), success.getRegistrationId());
   registrationListener.onRegistrationSuccess(this, success);
}
```

**ResourceManagerRegistrationListener.java**在TaskExecutor中

```java
public void onRegistrationSuccess(TaskExecutorToResourceManagerConnection connection, TaskExecutorRegistrationSuccess success) {
	final ResourceID resourceManagerId = success.getResourceManagerId();
	final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
	final ClusterInformation clusterInformation = success.getClusterInformation();
	final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();
	runAsync(
		() -> {
			// filter out outdated connections
			//noinspection ObjectEquality
			if (resourceManagerConnection == connection) {
				try {
					establishResourceManagerConnection(
						resourceManagerGateway,
						resourceManagerId,
						taskExecutorRegistrationId,
						clusterInformation);
				} catch (Throwable t) {
					log.error("Establishing Resource Manager connection in Task Executor failed", t);
				}
			}
		});
}
```

#### 9.注册slot

**ResourceManagerRegistrationListener.java**在TaskExecutor中

```java
private void establishResourceManagerConnection(
		ResourceManagerGateway resourceManagerGateway,
		ResourceID resourceManagerResourceId,
		InstanceID taskExecutorRegistrationId,
		ClusterInformation clusterInformation) {
	final CompletableFuture<Acknowledge> slotReportResponseFuture = resourceManagerGateway.sendSlotReport(
		getResourceID(),
		taskExecutorRegistrationId,
		taskSlotTable.createSlotReport(getResourceID()),
		taskManagerConfiguration.getTimeout());
	...	...
}
```

**ResourceManager.java**

```java
public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, InstanceID taskManagerRegistrationId, SlotReport slotReport, Time timeout) {
	final WorkerRegistration<WorkerType> workerTypeWorkerRegistration = taskExecutors.get(taskManagerResourceId);
	if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {
        // TODO by lwq 调用slotManager进行注册
		slotManager.registerTaskManager(workerTypeWorkerRegistration, slotReport);
		return CompletableFuture.completedFuture(Acknowledge.get());
	} else {
		...	...
	}
}
```

**SlotManagerImpl.java**

```java
public void registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
	checkInit();
	LOG.debug("Registering TaskManager {} under {} at the SlotManager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getI
	// we identify task managers by their instance id
	if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
		reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
	} else {
        // TODO by lwq 看第一次注册的过程
		// first register the TaskManager
		ArrayList<SlotID> reportedSlots = new ArrayList<>();
		for (SlotStatus slotStatus : initialSlotReport) {
			reportedSlots.add(slotStatus.getSlotID());
		}
        // TODO by lwq 构建注册对象
		TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
			taskExecutorConnection,
			reportedSlots);
		taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);
		// next register the new slots
		for (SlotStatus slotStatus : initialSlotReport) {
            // TODO by lwq 注册slot
			registerSlot(
				slotStatus.getSlotID(),
				slotStatus.getAllocationID(),
				slotStatus.getJobID(),
				slotStatus.getResourceProfile(),
				taskExecutorConnection);
		}
	}
}
```

```java
private void registerSlot(
		SlotID slotId,
		AllocationID allocationId,
		JobID jobId,
		ResourceProfile resourceProfile,
		TaskExecutorConnection taskManagerConnection) {
	if (slots.containsKey(slotId)) {
		// remove the old slot first
		removeSlot(
			slotId,
			new SlotManagerException(
				String.format(
					"Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
					slotId)));
	}
	// TODO by lwq 创建并注册slot
	final TaskManagerSlot slot = createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
	final PendingTaskManagerSlot pendingTaskManagerSlot;
	if (allocationId == null) {
		pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
	} else {
		pendingTaskManagerSlot = null;
	}
	// TODO by lwq 没有等待分配slot的任务
	if (pendingTaskManagerSlot == null) {
		updateSlot(slotId, allocationId, jobId);
	} else {
		pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
		final PendingSlotRequest assignedPendingSlotRequest = pendingTaskManagerSlot.getAssignedPendingSlotRequest();
		if (assignedPendingSlotRequest == null) {
			handleFreeSlot(slot);
		} else {
			// TODO by lwq 分配slot
			assignedPendingSlotRequest.unassignPendingTaskManagerSlot();
			allocateSlot(slot, assignedPendingSlotRequest);
		}
	}
}
```



#### 10.分配slot

**SlotManagerImpl.java**

```java
private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
	...	...
	TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);
	if (taskManagerRegistration == null) {
		throw new IllegalStateException("Could not find a registered task manager for instance id " +
			instanceID + '.');
	}
	// TODO by lwq 标记使用
	taskManagerRegistration.markUsed();
	// RPC call to the task manager
	// TODO by lwq RPC调用TaskManager的requestSlot方法
	CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
		slotId,
		pendingSlotRequest.getJobId(),
		allocationId,
		pendingSlotRequest.getResourceProfile(),
		pendingSlotRequest.getTargetAddress(),
		resourceManagerId,
		taskManagerRequestTimeout);
	...	...
}
```

**TaskExecutor.java**

```java
public CompletableFuture<Acknowledge> requestSlot(
	final SlotID slotId,
	final JobID jobId,
	final AllocationID allocationId,
	final ResourceProfile resourceProfile,
	final String targetAddress,
	final ResourceManagerId resourceManagerId,
	final Time timeout) {
	...	...
		if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
			// TODO by lwq 分配slot给job
			if (taskSlotTable.allocateSlot(slotId.getSlotNumber(), jobId, allocationId, resourceProfile, taskManagerConfiguration.getTimeout())) {
				log.info("Allocated slot for {}.", allocationId);
			} else {
				... ...
			}
		} else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
			...	...
		}
		...	
			offerSlotsToJobManager(jobId);
		...
}
```

**TaskSlotTableImpl.java**

```java
public boolean allocateSlot(
		int index,
		JobID jobId,
		AllocationID allocationId,
		ResourceProfile resourceProfile,
		Time slotTimeout) {
	checkRunning();
	Preconditions.checkArgument(index < numberSlots);
	TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
	if (taskSlot != null) {
		LOG.info("Allocation ID {} is already allocated in {}.", allocationId, taskSlot);
		return false;
	}
    // TODO by lwq 已经分配了
	if (taskSlots.containsKey(index)) {
		TaskSlot<T> duplicatedTaskSlot = taskSlots.get(index);
		LOG.info("Slot with index {} already exist, with resource profile {}, job id {} and allocation id {}.",
			index,
			duplicatedTaskSlot.getResourceProfile(),
			duplicatedTaskSlot.getJobId(),
			duplicatedTaskSlot.getAllocationId());
		return duplicatedTaskSlot.getJobId().equals(jobId) &&
			duplicatedTaskSlot.getAllocationId().equals(allocationId);
	} else if (allocatedSlots.containsKey(allocationId)) {
		return true;
	}
	resourceProfile = index >= 0 ? defaultSlotResourceProfile : resourceProfile;
	if (!budgetManager.reserve(resourceProfile)) {
		LOG.info("Cannot allocate the requested resources. Trying to allocate {}, "
				+ "while the currently remaining available resources are {}, total is {}.",
			resourceProfile,
			budgetManager.getAvailableBudget(),
			budgetManager.getTotalBudget());
		return false;
	}
	taskSlot = new TaskSlot<>(index, resourceProfile, memoryPageSize, jobId, allocationId);
	if (index >= 0) {
		taskSlots.put(index, taskSlot);
	}
	// update the allocation id to task slot map
	allocatedSlots.put(allocationId, taskSlot);
	// register a timeout for this slot since it's in state allocated
	timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());
	// add this slot to the set of job slots
    // TODO by lwq 记录slot分配给该job
	Set<AllocationID> slots = slotsPerJob.get(jobId);
	if (slots == null) {
		slots = new HashSet<>(4);
		slotsPerJob.put(jobId, slots);
	}
	slots.add(allocationId);
	return true;
}
```

#### 11.提供slot给JobManager

**TaskExecutor.java**

```java
public CompletableFuture<Acknowledge> requestSlot(
	final SlotID slotId,
	final JobID jobId,
	final AllocationID allocationId,
	final ResourceProfile resourceProfile,
	final String targetAddress,
	final ResourceManagerId resourceManagerId,
	final Time timeout) {
		...	
			offerSlotsToJobManager(jobId);
		...
}
```

```java
private void offerSlotsToJobManager(final JobID jobId) {
		... ...
			// TODO by lwq RPC调用，提供slot给JobManager
			CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
				getResourceID(),
				reservedSlots,
				taskManagerConfiguration.getTimeout());
			acceptedSlotsFuture.whenCompleteAsync(
				handleAcceptedSlotOffers(jobId, jobMasterGateway, jobMasterId, reservedSlots),
				getMainThreadExecutor());
		...	...
}
```

**JobMaster.java**

```java
public CompletableFuture<Collection<SlotOffer>> offerSlots(
      final ResourceID taskManagerId,
      final Collection<SlotOffer> slots,
      final Time timeout) {
   Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);
   if (taskManager == null) {
      return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
   }
   final TaskManagerLocation taskManagerLocation = taskManager.f0;
   final TaskExecutorGateway taskExecutorGateway = taskManager.f1;
   final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, getFencingToken());
   return CompletableFuture.completedFuture(
       // 提供slot
      slotPool.offerSlots(
         taskManagerLocation,
         rpcTaskManagerGateway,
         slots));
}
@Override
```

**SlotPoolImpl.java**

```java
public Collection<SlotOffer> offerSlots(
		TaskManagerLocation taskManagerLocation,
		TaskManagerGateway taskManagerGateway,
		Collection<SlotOffer> offers) {
	ArrayList<SlotOffer> result = new ArrayList<>(offers.size());
    // 提供slot
	for (SlotOffer offer : offers) {
		if (offerSlot(
			taskManagerLocation,
			taskManagerGateway,
			offer)) {
			result.add(offer);
		}
	}
	return result;
}
```

```java
boolean offerSlot(
		final TaskManagerLocation taskManagerLocation,
		final TaskManagerGateway taskManagerGateway,
		final SlotOffer slotOffer) {
	componentMainThreadExecutor.assertRunningInMainThread();
	// check if this TaskManager is valid
	final ResourceID resourceID = taskManagerLocation.getResourceID();
	final AllocationID allocationID = slotOffer.getAllocationId();
	if (!registeredTaskManagers.contains(resourceID)) {
		log.debug("Received outdated slot offering [{}] from unregistered TaskManager: {}",
				slotOffer.getAllocationId(), taskManagerLocation);
		return false;
	}
	// check whether we have already using this slot
	AllocatedSlot existingSlot;
	if ((existingSlot = allocatedSlots.get(allocationID)) != null ||
		(existingSlot = availableSlots.get(allocationID)) != null) {
		// we need to figure out if this is a repeated offer for the exact same slot,
		// or another offer that comes from a different TaskManager after the ResourceManager
		// re-tried the request
		// we write this in terms of comparing slot IDs, because the Slot IDs are the identifiers of
		// the actual slots on the TaskManagers
		// Note: The slotOffer should have the SlotID
		final SlotID existingSlotId = existingSlot.getSlotId();
		final SlotID newSlotId = new SlotID(taskManagerLocation.getResourceID(), slotOffer.getSlotIndex());
		if (existingSlotId.equals(newSlotId)) {
			log.info("Received repeated offer for slot [{}]. Ignoring.", allocationID);
			// return true here so that the sender will get a positive acknowledgement to the retry
			// and mark the offering as a success
			return true;
		} else {
			// the allocation has been fulfilled by another slot, reject the offer so the task executor
			// will offer the slot to the resource manager
			return false;
		}
	}
	final AllocatedSlot allocatedSlot = new AllocatedSlot(
		allocationID,
		taskManagerLocation,
		slotOffer.getSlotIndex(),
		slotOffer.getResourceProfile(),
		taskManagerGateway);
	// check whether we have request waiting for this slot
	PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
	if (pendingRequest != null) {
		// we were waiting for this!
		allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);
		if (!pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot)) {
			// we could not complete the pending slot future --> try to fulfill another pending request
			allocatedSlots.remove(pendingRequest.getSlotRequestId());
			tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
		} else {
			log.debug("Fulfilled slot request [{}] with allocated slot [{}].", pendingRequest.getSlotRequestId(), allocationID);
		}
	}
	else {
		// we were actually not waiting for this:
		//   - could be that this request had been fulfilled
		//   - we are receiving the slots from TaskManagers after becoming leaders
		tryFulfillSlotRequestOrMakeAvailable(allocatedSlot);
	}
	// we accepted the request in any case. slot will be released after it idled for
	// too long and timed out
	return true;
}
```

