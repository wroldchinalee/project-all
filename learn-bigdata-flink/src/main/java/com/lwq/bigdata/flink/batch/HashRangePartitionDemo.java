package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by Administrator on 2020-12-06.
 */
public class HashRangePartitionDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1, "hello1"));
        data.add(new Tuple2<>(2, "hello2"));
        data.add(new Tuple2<>(2, "hello3"));
        data.add(new Tuple2<>(3, "hello4"));
        data.add(new Tuple2<>(3, "hello5"));
        data.add(new Tuple2<>(3, "hello6"));
        data.add(new Tuple2<>(4, "hello7"));
        data.add(new Tuple2<>(4, "hello8"));
        data.add(new Tuple2<>(4, "hello9"));
        data.add(new Tuple2<>(4, "hello10"));
        data.add(new Tuple2<>(5, "hello11"));
        data.add(new Tuple2<>(5, "hello12"));
        data.add(new Tuple2<>(5, "hello13"));
        data.add(new Tuple2<>(5, "hello14"));
        data.add(new Tuple2<>(5, "hello15"));
        data.add(new Tuple2<>(6, "hello16"));
        data.add(new Tuple2<>(6, "hello17"));
        data.add(new Tuple2<>(6, "hello18"));
        data.add(new Tuple2<>(6, "hello19"));
        data.add(new Tuple2<>(6, "hello20"));
        data.add(new Tuple2<>(6, "hello21"));
        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        // 这种分组不均匀
        MapPartitionOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> mapPartition = text.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> iterator = values.iterator();
                while (iterator.hasNext()) {
                    Tuple2<Integer, String> tuple2 = iterator.next();
                    System.out.printf("当前线程id:%s,数据:%s\n", Thread.currentThread().getId(), tuple2);
                }
            }
        });
        mapPartition.print();
        System.out.println("--------------------------------------------");

        text.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> iterator = values.iterator();
                while (iterator.hasNext()) {
                    Tuple2<Integer, String> tuple2 = iterator.next();
                    System.out.printf("当前线程id:%s,数据:%s\n", Thread.currentThread().getId(), tuple2);
                }
            }
        }).print();
    }
}
