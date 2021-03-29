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
2. 构造java命令运行所需参数
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

