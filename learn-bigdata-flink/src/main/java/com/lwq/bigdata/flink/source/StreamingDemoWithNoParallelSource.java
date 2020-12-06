package com.lwq.bigdata.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Administrator on 2020-12-06.
 * 从自定义的数据源里面获取数据，然后过滤出偶数
 */
public class StreamingDemoWithNoParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = environment.addSource(new MyNoParallelSource());

        SingleOutputStreamOperator<Long> streamOperator = streamSource.map(value -> {
            System.out.printf("接收到了数据%d\n", value);
            return value;
        }).filter(value -> value % 2 == 0);

        streamOperator.print();//输出默认是cpu核数个并行度
        environment.execute("StreamingDemoWithNoParallelSource");
    }
}
