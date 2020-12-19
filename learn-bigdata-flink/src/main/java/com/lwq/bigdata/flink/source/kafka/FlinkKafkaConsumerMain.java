package com.lwq.bigdata.flink.source.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by Administrator on 2020-12-17.
 */
public class FlinkKafkaConsumerMain {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.enableCheckpointing(5000);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 如果你是0.8版本的Kafka，需要配置

        //properties.setProperty("zookeeper.connect", "localhost:2181");

        //设置消费组

        properties.setProperty("group.id", "group_test");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);

        //设置从最早的ffset消费

        consumer.setStartFromEarliest();

        //还可以手动指定相应的 topic, partition，offset,然后从指定好的位置开始消费

        //HashMap<KafkaTopicPartition, Long> map = new HashMap<>();

        //map.put(new KafkaTopicPartition("test", 1), 10240L);

        //假如partition有多个，可以指定每个partition的消费位置

        //map.put(new KafkaTopicPartition("test", 2), 10560L);

        //然后各个partition从指定位置消费

        //consumer.setStartFromSpecificOffsets(map);

        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {

            @Override

            public void flatMap(String value, Collector<String> out) throws Exception {

                System.out.println(value);

            }

        });

        env.execute("start consumer...");

    }

}
