### Flink Kafka Connector源码



继承关系

```java
RichParallelSourceFunction，CheckpointListener，ResultTypeQueryable，CheckpointedFunction
    FlinkKafkaConsumerBase继承了上面的几个接口
    	FlinkKafkaConsumer09继承了FlinkKafkaConsumerBase
    		FlinkKafkaConsumer010继承了FlinkKafkaConsumer09
```



```
public interface CheckpointedFunction {
   // checkpoint时调用存储状态
   void snapshotState(FunctionSnapshotContext context) throws Exception;
   // 初始化状态
   void initializeState(FunctionInitializationContext context) throws Exception;

}
```



```
// checkpoint完成时的回调函数
public interface CheckpointListener {
   void notifyCheckpointComplete(long checkpointId) throws Exception;
}
```

核心方法：

FlinkKafkaConsumerBase.run(SourceContext<T> sourceContext)

实现了SourceFunction的run方法，定义了如何产生数据



AbstractFetcher类

这个类里面包含了统计和水印相关的变量，还有提交偏移量的方法。

最关键的方法是public abstract void runFetchLoop() throws Exception;

Fetcher继承关系

```java
AbstractFetcher
	Kafka09Fetcher
		Kafka010Fetcher
```

Kafka09Fetcher类

创建了3个比较重要的成员变量，deserializer，handover和consumerThread。





KafkaConsumerThread类

作为真正的数据生产者，是一个Runnable对象，创建了kafka的consumer对象

通过consumer对象获取数据，并且将数据交给handover对象，下游通过handover

对象获取数据



./bin/flink run -m yarn-cluster -ys 1 -yjm 1G -ytm 2G -d -c com.lwq.bigdata.flink.state.ValueStateWithCheckpoint examples/learn-bigdata-flink-1.0-SNAPSHOT.jar

./bin/flink savepoint jobid -d hdfs目录 -yid yarn应用id 



对任务进行savepoint

```bash
./bin/flink savepoint b7f7ec83e92de3e9735c13597cb06032  hdfs://bigdata01:9000/users/master/flink/savepoints -yid application_1615001521320_0008
```



从savepoint恢复任务

```bash
./bin/flink run \
	  -m yarn-cluster -ys 1 -yjm 1G -ytm 2G -d \
	  -c com.lwq.bigdata.flink.state.ValueStateWithCheckpoint \
      --fromSavepoint hdfs://bigdata01:9000/users/master/flink/savepoints/savepoint-b7f7ec-c6e0d100da0e \
      ./examples/learn-bigdata-flink-1.0-SNAPSHOT.jar
```

```
2021-03-07 17:26:40,366 INFO  com.lwq.bigdata.flink.state.ValueStateWithCheckpoint          - MyProcessFunction snapshotState partition:0,offset:4
2021-03-07 17:26:40,366 INFO  com.lwq.bigdata.flink.state.ValueStateWithCheckpoint          - MyProcessFunction snapshotState partition:1,offset:1
```