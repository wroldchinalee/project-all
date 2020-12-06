package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-06.
 * 笛卡尔积
 */
public class CrossDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> list1 = new ArrayList<>();
        list1.add("zs");
        list1.add("ls");
        ArrayList<Integer> list2 = new ArrayList<>();
        list2.add(1);
        list2.add(2);

        DataSource<String> text1 = environment.fromCollection(list1);
        DataSource<Integer> text2 = environment.fromCollection(list2);

        text1.cross(text2).print();
    }
}
