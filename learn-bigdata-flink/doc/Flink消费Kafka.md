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

