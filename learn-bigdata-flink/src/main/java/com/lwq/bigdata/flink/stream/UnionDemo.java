package com.lwq.bigdata.flink.stream;

import com.lwq.bigdata.flink.source.MyNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Administrator on 2020-12-06.
 * 合并多个流，新的流会包含所有流中的数据，但是union是一个限制，就是所有合并的流类型必须是一致的
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource1 = environment.addSource(new MyNoParallelSource());
        DataStreamSource<Long> streamSource2 = environment.addSource(new MyNoParallelSource());
        DataStream<Long> unionStream = streamSource1.union(streamSource2);
        SingleOutputStreamOperator<Long> numStream = unionStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.printf("接收到的数据:%d\n", value);
                return value;
            }
        });
        SingleOutputStreamOperator<Long> outputStreamOperator = numStream.timeWindowAll(Time.seconds(2)).sum(0);
        outputStreamOperator.print();
        environment.execute("UnionDemo");

    }

}
