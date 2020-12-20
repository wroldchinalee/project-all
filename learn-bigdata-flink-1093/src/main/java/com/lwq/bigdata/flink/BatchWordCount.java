package com.lwq.bigdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-11-26.
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = environment.readTextFile("F:\\src\\tuling\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\hello.txt");
        AggregateOperator<Tuple2<String, Integer>> sum = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (int i = 0; i < arr.length; i++) {
                    collector.collect(new Tuple2<String, Integer>(arr[i].trim(), 1));
                }
            }
        }).groupBy(0).sum(1);

        sum.print();
    }
}
