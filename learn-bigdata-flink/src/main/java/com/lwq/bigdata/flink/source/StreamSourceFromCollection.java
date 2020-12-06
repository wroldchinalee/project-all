package com.lwq.bigdata.flink.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-05.
 * 数据源为集合中的元素
 */
public class StreamSourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> list = new ArrayList<String>();
        list.add("hadoop");
        list.add("flink");
        list.add("bigdata");
        DataStreamSource<String> source = environment.fromCollection(list);
        SingleOutputStreamOperator<String> streamOperator = source.map(new MapFunction<String, String>() {
            public String map(String s) throws Exception {
                return "lwq_" + s;
            }
        });

        streamOperator.print();
        environment.execute("StreamSourceFromCollection Job");
    }
}
