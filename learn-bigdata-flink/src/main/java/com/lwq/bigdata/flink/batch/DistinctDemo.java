package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-06.
 */
public class DistinctDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> list = new ArrayList<>();
        list.add("you jump");
        list.add("I jump");
        DataSource<String> dataSource = environment.fromCollection(list);
        FlatMapOperator<String, Tuple2<String, Long>> wordCountDataSet = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] arr = value.split(" ");
                for (int i = 0; i < arr.length; i++) {
                    out.collect(Tuple2.of(arr[i].trim(), 1L));
                }
            }
        });
        wordCountDataSet.distinct().print();

    }
}
