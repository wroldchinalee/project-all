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