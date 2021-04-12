package com.lwq.bigdata.flink.window;

import com.lwq.bigdata.flink.mix.MySource1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

public class GlobalWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputSource = env.addSource(new MySource1());
        inputSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                for (int i = 0; i < split.length; i++) {
                    out.collect(Tuple2.of(split[i], 1L));
                }
            }
        }).keyBy(0).window(GlobalWindows.create()).trigger(CountTrigger.of(3)).sum(1).print();
        env.execute(GlobalWindow.class.getSimpleName());
    }
}
