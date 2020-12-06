package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-06.
 */
public class JoinDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(Tuple2.of(1, "zs"));
        list1.add(Tuple2.of(2, "ls"));
        list1.add(Tuple2.of(3, "ww"));
        ArrayList<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(Tuple2.of(1, "beijing"));
        list2.add(Tuple2.of(2, "shanghai"));
        list2.add(Tuple2.of(3, "guangzhou"));

        DataSource<Tuple2<Integer, String>> dataSource1 = environment.fromCollection(list1);
        DataSource<Tuple2<Integer, String>> dataSource2 = environment.fromCollection(list2);

        // 指定第一个数据集中需要进行比较的元素位置
        dataSource1.join(dataSource2).where(0)
                // 制定第二个数据集中需要进行比较的元素位置
                .equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        }).print();
        System.out.println("==================================");
        //注意，这里用map和上面使用的with最终效果是一致的
//        dataSource1.join(dataSource2).where(0)
//                .equalTo(0).map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer, String, String>>() {
//            @Override
//            public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
//
//                return new Tuple3<>(value.f0.f0, value.f0.f1, value.f1.f1);
//            }
//        }).print();
    }
}
