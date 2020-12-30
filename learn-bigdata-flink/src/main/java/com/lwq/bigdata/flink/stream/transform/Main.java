package com.lwq.bigdata.flink.stream.transform;

import com.lwq.bigdata.flink.tablesql.RowSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/15
 * @description: Main
 **/
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> streamSource = env.addSource(new RowSource());
        Transformer transformer = new Transformer("", env);
        DataStream<Row> resultStream = transformer.transform(streamSource);
        resultStream.print();
        env.execute();
    }
}
