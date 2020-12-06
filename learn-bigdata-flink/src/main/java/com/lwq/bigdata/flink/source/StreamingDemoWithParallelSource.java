package com.lwq.bigdata.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Administrator on 2020-12-06.
 */
public class StreamingDemoWithParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为2
        DataStreamSource<Long> streamSource = environment.addSource(new MyParallelSource()).setParallelism(2);
        // 第一个参数为输入数据的类型，第二个参数为输出数据的类型
        SingleOutputStreamOperator<Long> outputStreamOperator = streamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.printf("接收到了数据%d\n", value);
                return value;
            }
            // 参数为输入参数的类型
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });
        outputStreamOperator.print();
        environment.execute("StreamingDemoWithParallelSource");

    }
}
