package com.lwq.bigdata.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by Administrator on 2020-12-05.
 * 创建并行度为1的自定义数据源，继承SourceFunction接口
 * 指定数据类型
 * 每秒产生一条数据
 */
public class MyNoParallelSource implements SourceFunction<Long> {
    private boolean isRunning = true;
    private long number;

    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            number++;
            ctx.collect(number);
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        isRunning = true;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource source = environment.addSource(new MyNoParallelSource()).setParallelism(1);
        source.print("打印");//输出默认是cpu核数的并行度
        environment.execute("MyNoParallelSource");

    }
}
