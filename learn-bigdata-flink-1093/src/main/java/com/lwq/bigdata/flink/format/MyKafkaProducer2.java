package com.lwq.bigdata.flink.format;

import org.apache.kafka.clients.producer.KafkaProducer;
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
public class MyKafkaProducer2 {
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

        String topic = "test2";
        Random random = new Random();
        while (running) {

            int x = random.nextInt(10) + 1;
            int y = random.nextInt(10) + 1;
            int x2 = random.nextInt(10) + 1;
            int y2 = random.nextInt(10) + 1;
            int x3 = random.nextInt(10) + 1;
            int y3 = random.nextInt(10) + 1;
            String valueStr = String.format("{\"__db\":\"dcbsdb\",\"data\":[{\"x\":\"%s\",\"y\":\"%s\"},{\"x\":\"%s\",\"y\":\"%s\"},{\"x\":\"%s\",\"y\":\"%s\"}]}", x, y, x2, y2, x3, y3);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, valueStr);
            producer.send(record);
            Thread.sleep(1000);
        }


        producer.close();

        System.out.println("producer end");
    }

    public static String JSON_SCHEMA = "{\"__db\":\"dcbsdb\",\"data\":[{\"x\":2,\"y\":\"8\"},{\"x\":\"3\",\"y\":\"7\"},{\"x\":\"4\",\"y\":6}]}";
}
