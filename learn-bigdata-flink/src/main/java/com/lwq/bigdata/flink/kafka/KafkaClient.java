package com.lwq.bigdata.flink.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: LWQ
 * @create: 2020/12/18
 * @description: KafkaClient
 **/
public class KafkaClient {
    private static boolean running = true;

    public static void main(String[] args) throws InterruptedException {
        //配置kafka参数
        Properties properties = new Properties();
        //0.8版本的需要zk.connect
        properties.put("zk.connect", "192.168.233.130:2181");
        properties.put("bootstrap.servers", "192.168.233.130:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);


        String topic = "test";
        String value = "{\"__db\":\"dcbsdb\",\"data\":[{\"id\":\"1\"}]}";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        while (running) {
            producer.send(record);
            Thread.sleep(1000);
        }


        producer.close();

        System.out.println("producer end");
    }

}
