package com.lwq.bigdata.flink.window;

import com.lwq.bigdata.flink.mix.MySource1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SessionWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.addSource(new MySource1());
        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                for (int i = 0; i < split.length; i++) {
                    out.collect(Tuple2.of(split[i], 1L));
                }
            }
        }).keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        String s = String.format("word:%s,count:%s,currTime:%s",
                                value.f0, value.f1, ctx.timestamp());
                        out.collect(s);
                    }
                }).print();
        env.execute(SessionWindowTest.class.getSimpleName());
    }
}
