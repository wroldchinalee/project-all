### Kafka命令

创建topic

```shell
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```



生产者

```shell
./bin/kafka-console-producer.sh --broker-list bigdata01:9092 --topic test
```



查询所有topic

```shell
./bin/kafka-topics.sh --zookeeper bigdata01:2181/kafka  --list 
```



查询某个topic信息

```shell
./bin/kafka-topics.sh --zookeeper bigdata01:2181/kafka --topic topic_name --describe
```



消费者

```shell
./bin/kafka-console-consumer.sh --zookeeper bigdata01:2181  --topic test
```



查看消费者组

```shell
./bin/kafka-consumer-groups.sh --new-consumer --bootstrap-server bigdata01:9092 --list
```



查看消费组详情

```
./kafka-consumer-groups.sh --new-consumer --bootstrap-server bigdata01:9092 --describe --group group1
```

