package com.lwq.bigdata.flink.stream;

import com.lwq.bigdata.flink.source.MyNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Created by Administrator on 2020-12-06.
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 * 需要和CoMap，CoFlatMap函数配合使用
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource1 = environment.addSource(new MyNoParallelSource());
        DataStreamSource<Long> streamSource2 = environment.addSource(new MyNoParallelSource());
        SingleOutputStreamOperator<String> strStreamSource = streamSource2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });
        // 连接两个不同类型的数据流
        SingleOutputStreamOperator<Object> outputStreamOperator = streamSource1.connect(strStreamSource).map(new CoMapFunction<Long, String, Object>() {
            // 分别使用不同的处理方法
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });
        outputStreamOperator.print().setParallelism(1);
        environment.execute("ConnectDemo");
    }
}
