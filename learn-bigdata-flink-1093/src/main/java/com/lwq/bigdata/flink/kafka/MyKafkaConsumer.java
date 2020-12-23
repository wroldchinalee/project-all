package com.lwq.bigdata.flink.kafka;

import com.lwq.bigdata.flink.format.FlinkKafkaSqlMain;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: LWQ
 * @create: 2020/12/19
 * @description: MyKafkaConsumer
 **/
public class MyKafkaConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        // 必须设置的属性
        props.put("bootstrap.servers", "192.168.233.130:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");

        // 可选设置属性
        props.put("enable.auto.commit", "true");
        // 自动提交offset,每1s提交一次
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest ");
        props.put("client.id", "zy_client_id");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅test1 topic
        consumer.subscribe(Collections.singletonList("test"));

        String jsonSchema = FlinkKafkaSqlMain.jsonSchema2;
        JsonRowDeserializationSchema schema = new JsonRowDeserializationSchema.Builder(jsonSchema).failOnMissingField().build();
        while (true) {
            //  从服务器开始拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
//                System.out.printf("topic = %s ,partition = %d,offset = %d, key = %s, value = %s%n", record.topic(), record.partition(),
//                        record.offset(), record.key(), record.value());
                try {
                    Row row = schema.deserialize(record.value().getBytes("UTF-8"));
                    System.out.printf("row:%s\n", row);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
