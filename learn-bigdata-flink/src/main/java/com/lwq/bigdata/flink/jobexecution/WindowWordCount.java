package com.lwq.bigdata.flink.jobexecution;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.readTextFile(params.get("input")).setParallelism(2);
        env.getConfig().setGlobalJobParameters(params);
        final int windowSize = params.getInt("window", 10);
        final int slideSize = params.getInt("slide",5);

        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).setParallelism(4).slotSharingGroup("flatMap_sg")
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1).setParallelism(3).slotSharingGroup("sum_sg");
        counts.print().setParallelism(3);
        env.execute("WindowWordCount");

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
