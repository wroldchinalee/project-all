package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * Created by Administrator on 2020-12-06.
 * <p>
 * Accumulator即累加器，与Mapreduce counter的应用场景差不多，都能很好地观察task在运行期间的数
 * 据变化
 * 可以在Flink job任务中的算子函数中操作累加器，但是只能在任务执行结束之后才能获得累加器的最终结
 * 果。
 * Counter是一个具体的累加器(Accumulator)实现
 * IntCounter, LongCounter 和 DoubleCounter
 * 用法
 * 1：创建累加器
 * private IntCounter numLines = new IntCounter();六 、总结（5分钟）
 * 2：注册累加器
 * getRuntimeContext().addAccumulator("num-lines", this.numLines);
 * 3：使用累加器
 * this.numLines.add(1);
 * 4：获取累加器的结果
 * myJobExecutionResult.getAccumulatorResult("num-lines")
 */
public class CounterDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = environment.fromElements("a", "b", "c", "d");
        MapOperator<String, String> result = dataSource.map(new RichMapFunction<String, String>() {
            // 1.创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2.注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                // 如果并行度为1，使用普通的累加求和即可，但是设置多个并行度，则普通的累加求和
                // 结果就不准了
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(2);

        // 如果要获取counter的值，只能是任务
        result.writeAsText("F:\\src\\tuling\\project-all\\learn-bigdata-flink\\src\\main\\resources");
        JobExecutionResult jobExecutionResult = environment.execute("counter");
        int num = jobExecutionResult.getAccumulatorResult("num-lines");
        System.out.println("num:" + num);

    }

}
