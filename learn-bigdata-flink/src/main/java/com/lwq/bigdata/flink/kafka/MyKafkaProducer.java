package com.lwq.bigdata.flink.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * @author: LWQ
 * @create: 2020/12/18
 * @description: MyKafkaProducer
 **/
public class MyKafkaProducer {
    private static boolean running = true;

    public static void main(String[] args) throws InterruptedException {
        //配置kafka参数
        Properties properties = new Properties();
        //0.8版本的需要zk.connect
        properties.put("zk.connect", "192.168.233.130:2181");
        properties.put("bootstrap.servers", "192.168.233.130:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);


        String topic = "test";
//        String value = "{\"__db\":\"dcbsdb\",\"data\":[{\"id\":\"1\"}]}";
        Random random = new Random();
        while (running) {
            int id = random.nextInt(10) + 1;
            String value = "{\"__db\":\"dcbsdb\",\"data\":[{\"id\":\"" + id + "\"}]}";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            producer.send(record);
            Thread.sleep(1000);
        }


        producer.close();

        System.out.println("producer end");
    }

}
