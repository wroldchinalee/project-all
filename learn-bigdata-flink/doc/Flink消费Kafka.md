Flink消费Kafka



1. 我们使用FlinkKafkaConumser,并且启用Checkpoint，偏移量会通过checkpoint保存到state里
   面，并且默认会写入到kafka的特殊主体中，也就是__consumer_offset
2. setCommitOffsetsOnCheckpoints 默认会true，就是把偏移量写入特殊主题中
3. Flink自动重启的过程中，读取的偏移量是state中的偏移量
4. 我们手动重启任务的过程中，默认读取的是__consumer_offset中的偏移量，如果
   _consumer_offset里面没有那么默认就会从earliest读取数据  

```java
consumer.setCommitOffsetsOnCheckpoints(false)//默认为true
```



FlinkKafkaConsumerBase

```java
public abstract class FlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T> implements
		CheckpointListener,
		ResultTypeQueryable<T>,
		CheckpointedFunction{
		
	xxxxxxxxxxxxxxxxxx
		}
```

主要继承了RichParallelSourceFunction，CheckpointListener和CheckpointedFunction。



主要方法run，里面主要是实现了如何从kafka读取数据的。

里面创建了一个AbstractFetcher，然后执行了这个类的runFetchLoop方法。

这个方法的具体实现如下：

```java
public void runFetchLoop() throws Exception {
		try {
            // 获取数据，实际与kafka进行交互获取数据的实现在consumerThread这个类
			final Handover handover = this.handover;

			// kick off the actual Kafka consumer
			consumerThread.start();

			while (running) {
				// this blocks until we get the next records
				// it automatically re-throws exceptions encountered in the consumer thread
                // 获取数据
				final ConsumerRecords<byte[], byte[]> records = handover.pollNext();

				// get the records for each topic partition
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {

					List<ConsumerRecord<byte[], byte[]>> partitionRecords =
							records.records(partition.getKafkaPartitionHandle());

					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {

						final T value = deserializer.deserialize(record);

						if (deserializer.isEndOfStream(value)) {
							// end of stream signaled
							running = false;
							break;
						}

						// emit the actual record. this also updates offset state atomically
						// and deals with timestamps and watermark generation
						emitRecord(value, partition, record.offset(), record);
					}
				}
			}
		}
		finally {
			// this signals the consumer thread that no more work is to be done
			consumerThread.shutdown();
		}

		// on a clean exit, wait for the runner thread
		try {
			consumerThread.join();
		}
		catch (InterruptedException e) {
			// may be the result of a wake-up interruption after an exception.
			// we ignore this here and only restore the interruption state
			Thread.currentThread().interrupt();
		}
	}
```

这个方法主要做了以下几件事：

1.不断地从handover这个中转对象获取kafka数据，handover对象的数据是从consumerThread中产生的

使用了生产者-消费者模式。

2.将从kafka获得的数据还有offset的状态变量，record等信息一起发送到下游

3.AbstractFetch的成员变量中有SourceContext这个对象，通过这个对象将数据发送到下游



关于AbstractFetcher类checkpoint相关的还有两个比较重要的成员变量

如下：

```java
private final Object checkpointLock;

/** All partitions (and their state) that this fetcher is subscribed to. */
private final List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates;
```

第一个是和checkpoint有关的对象锁，

第二个是这个fetcher订阅的所有分区和它的状态信息

KafkaTopicPartitionState这个类的实现如下：

```java
public class KafkaTopicPartitionState<KPH> {

	// ------------------------------------------------------------------------

	/** The Flink description of a Kafka partition. */
	private final KafkaTopicPartition partition;

	/** The Kafka description of a Kafka partition (varies across different Kafka versions). */
	private final KPH kafkaPartitionHandle;

	/** The offset within the Kafka partition that we already processed. */
	private volatile long offset;

	/** The offset of the Kafka partition that has been committed. */
	private volatile long committedOffset;
}
```

主要有4个变量，含义分别如下：

1.flink定义的kafka的partition类

2.kafka定义的partition类，根据不同的kafka版本，可能会有所不同

3.我们已经处理的kafka offset

4.我们已经提交的kafka offset



source在向下游发送数据时会调用emitRecord方法，

最终会调用到下面这个方法，代码如下：

```java
protected void emitRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) throws Exception {

   if (record != null) {
      if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
         // fast path logic, in case there are no watermarks

         // emit the record, using the checkpoint lock to guarantee
         // atomicity of record emission and offset state update
         synchronized (checkpointLock) {
            sourceContext.collect(record);
            partitionState.setOffset(offset);
         }
      } else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
         emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, Long.MIN_VALUE);
      } else {
         emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, Long.MIN_VALUE);
      }
   } else {
      // if the record is null, simply just update the offset state for partition
      synchronized (checkpointLock) {
         partitionState.setOffset(offset);
      }
   }
}
```

我们假设是没有watermark的情况

那么主要的逻辑就是调用sourceContext往下游发送数据，并且更新KafkaTopicPartitionState中的offset值。



前面分析了是如何更新kafka的offset状态的。

下面继续分析一下，KafkaConsumer是怎么进行快照的。

我们直接看FlinkKafkaConsumerBase类的snapshot方法

```java
public final void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
		} else {
			unionOffsetStates.clear();

			final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
			if (fetcher == null) {
				// the fetcher has not yet been initialized, which means we need to return the
				// originally restored offsets or the assigned partitions
				for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition : subscribedPartitionsToStartOffsets.entrySet()) {
					unionOffsetStates.add(Tuple2.of(subscribedPartition.getKey(), subscribedPartition.getValue()));
				}

				if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
					// the map cannot be asynchronously updated, because only one checkpoint call can happen
					// on this function at a time: either snapshotState() or notifyCheckpointComplete()
					pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
				}
			} else {
                // 重要，获取当前的状态
				HashMap<KafkaTopicPartition, Long> currentOffsets = fetcher.snapshotCurrentState();

				if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
					// the map cannot be asynchronously updated, because only one checkpoint call can happen
					// on this function at a time: either snapshotState() or notifyCheckpointComplete()
					pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
				}

				for (Map.Entry<KafkaTopicPartition, Long> kafkaTopicPartitionLongEntry : currentOffsets.entrySet()) {
					unionOffsetStates.add(
						Tuple2.of(kafkaTopicPartitionLongEntry.getKey(), kafkaTopicPartitionLongEntry.getValue()));
				}
			}

			if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
				// truncate the map of pending offsets to commit, to prevent infinite growth
				while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
					pendingOffsetsToCommit.remove(0);
				}
			}
		}
	}
```

上面的代码逻辑，我们直接看fetcher不为null的情况。

这种情况的主要逻辑是：

1.获取当前状态

2.将当前checkpoint加入到pending列表

3.将状态变量的值放入到flink的状态对象unionOffsetStates中



我们再看一下是如何获取当前状态的：

```java
	public HashMap<KafkaTopicPartition, Long> snapshotCurrentState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		HashMap<KafkaTopicPartition, Long> state = new HashMap<>(subscribedPartitionStates.size());
		for (KafkaTopicPartitionState<KPH> partition : subscribedPartitionStates) {
			state.put(partition.getKafkaTopicPartition(), partition.getOffset());
		}
		return state;
	}
```

1.首先判断是否持有checkpointLock

2.如果持有锁，那么将subscribedPartitionStates变量的值赋值给新的state对象中



AbstractFetcher中的checkpointLock

是通过创建对象的时候传入的sourceContext对象的sourceContext.getCheckpointLock();获取的

追到最上层其实就是StreamSourceTask中的getCheckpointLock()方法获得的

堆栈调用如下：

```java
LegacySourceFunctionThread.run()
  headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);
    StreamSource.run(final Object lockingObject,
			final StreamStatusMaintainer streamStatusMaintainer,
			final Output<StreamRecord<OUT>> collector,
			final OperatorChain<?, ?> operatorChain)
	  this.ctx = StreamSourceContexts.getSourceContext(
			timeCharacteristic,
			getProcessingTimeService(),
			lockingObject,
			streamStatusMaintainer,
			collector,
			watermarkInterval,
			-1)
	    ctx = new ManualWatermarkContext<>(
					output,
					processingTimeService,
					checkpointLock,
					streamStatusMaintainer,
					idleTimeout);
		  super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);
		    this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "Checkpoint Lock cannot be null.");
		      AbstractFetcher()构造方法
		      	this.checkpointLock = sourceContext.getCheckpointLock();
```

其中第二行getCheckpointLock方法是从哪里获取锁的呢？

代码如下：

```java
	@Deprecated
	public Object getCheckpointLock() {
		return actionExecutor.getMutex();
	}
```

调用的其实是StreamTask类的getCheckpointLock方法，从actionExecution的getMutex获取。





snapshot同步加锁的代码过程：

直接看StreamTask的performCheckpoint方法

核心代码如下：

```java
if (isRunning) {
    //TODO 重要，这个方法里面会加锁
			actionExecutor.runThrowing(() -> {

				if (checkpointOptions.getCheckpointType().isSynchronous()) {
					setSynchronousSavepointId(checkpointId);

					if (advanceToEndOfTime) {
						advanceToEndOfEventTime();
					}
				}

				// All of the following steps happen as an atomic step from the perspective of barriers and
				// records/watermarks/timers/callbacks.
				// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
				// checkpoint alignments

				// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
				//           The pre-barrier work should be nothing or minimal in the common case.
				operatorChain.prepareSnapshotPreBarrier(checkpointId);

				// Step (2): Send the checkpoint barrier downstream
				operatorChain.broadcastCheckpointBarrier(
						checkpointId,
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				// Step (3): Take the state snapshot. This should be largely asynchronous, to not
				//           impact progress of the streaming topology
				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);

			});

			return true;
		}
```

runThrowing的方法具体实现如下：

```java
	@Override
		public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E {
			synchronized (mutex) {
				runnable.run();
			}
		}

```

这里会通过mutex对象加锁，整个runnable的run方法在同步代码块中。

这个mutex对象是SynchronizedStreamTaskActionExecutor的变量private final Object mutex;



从这里我们可以得到结论，我们更新offset的state的时候和snapshot的时候使用的是同一把锁。

emitRecord的时候使用的checkpointLock是从sourceContext获取的，而sourceContext的锁其实是StreamTask里面的mutex，实际上是同一把锁。

所以在snapshot的时候，整个数据流是暂停的，他们被同一个锁对象同步了，只有在checkpoint完成之后才会继续进行emitRecord。