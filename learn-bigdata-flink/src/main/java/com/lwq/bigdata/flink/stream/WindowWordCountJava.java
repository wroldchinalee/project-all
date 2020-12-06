package com.lwq.bigdata.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-12-06.
 */
public class WindowWordCountJava {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = environment.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Long>> outputStreamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (int i = 0; i < arr.length; i++) {
                    collector.collect(Tuple2.of(arr[i].trim(), 1L));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(2), Time.seconds(1)).sum(1);
        outputStreamOperator.print();
        environment.execute("WindowWordCountJava");
    }
}
