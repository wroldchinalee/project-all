package com.lwq.bigdata.flink.sink;

import com.lwq.bigdata.flink.source.MyNoParallelSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Administrator on 2020-12-06.
 * <p>
 * <p>
 * 数据源：1 2 3 4 5.....源源不断过来
 * 通过map打印一下接受到数据
 * 通过filter过滤一下数据，我们只需要偶数
 */
public class WriteTextDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = environment.addSource(new MyNoParallelSource());

        SingleOutputStreamOperator<Long> streamOperator = streamSource.map(value -> {
            System.out.printf("接收到了数据%d\n", value);
            return value;
        }).filter(value -> value % 2 == 0);

        streamOperator.writeAsText("F:\\src\\tuling\\project-all\\learn-bigdata-flink\\src\\main\\resources").setParallelism(1);

        environment.execute(WriteTextDemo.class.getSimpleName());
    }
}
