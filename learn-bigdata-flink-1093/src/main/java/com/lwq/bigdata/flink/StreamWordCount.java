package com.lwq.bigdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-11-26.
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputStream = environment.readTextFile("F:\\src\\tuling\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\hello.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = inputStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (int i = 0; i < arr.length; i++) {
                    collector.collect(new Tuple2<String, Integer>(arr[i].trim(), 1));
                }
            }
        }).keyBy(0).sum(1);

        sum.print();
        environment.execute("stream word count");
    }
}
