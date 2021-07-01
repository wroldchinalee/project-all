package com.lwq.bigdata.flink.mix;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-12-19.
 */
public class TestWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.addSource(new MySource1());
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                for (int i = 0; i < split.length; i++) {
                    out.collect(Tuple2.of(split[i], 1L));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5)).sum(1).print();
        env.execute("TestWindow");
    }
}
