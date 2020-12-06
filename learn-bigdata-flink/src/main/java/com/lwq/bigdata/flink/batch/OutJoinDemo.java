package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-06.
 */
public class OutJoinDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(Tuple2.of(1, "zs"));
        list1.add(Tuple2.of(2, "ls"));
        list1.add(Tuple2.of(3, "ww"));
        ArrayList<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(Tuple2.of(1, "beijing"));
        list2.add(Tuple2.of(2, "shanghai"));
        list2.add(Tuple2.of(4, "guangzhou"));

        DataSource<Tuple2<Integer, String>> text1 = environment.fromCollection(list1);
        DataSource<Tuple2<Integer, String>> text2 = environment.fromCollection(list2);

        // 左外连接
        text1.leftOuterJoin(text2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second == null) {
                            return Tuple3.of(first.f0, first.f1, null);
                        } else {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();


        text1.rightOuterJoin(text2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return Tuple3.of(second.f0, null, second.f1);
                        } else {
                            return Tuple3.of(second.f0, first.f1, second.f1);
                        }
                    }
                }).print();

        text1.fullOuterJoin(text2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return Tuple3.of(second.f0, null, second.f1);
                        } else if (second == null) {
                            return Tuple3.of(first.f0, first.f1, null);
                        } else {
                            return Tuple3.of(second.f0, first.f1, second.f1);
                        }
                    }
                }).print();
    }
}
