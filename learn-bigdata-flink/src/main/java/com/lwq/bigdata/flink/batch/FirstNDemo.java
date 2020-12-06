package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-06.
 */
public class FirstNDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2, "zs"));
        data.add(new Tuple2<>(4, "ls"));
        data.add(new Tuple2<>(3, "ww"));
        data.add(new Tuple2<>(1, "xw"));
        data.add(new Tuple2<>(1, "aw"));
        data.add(new Tuple2<>(1, "mw"));
        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        // 获取前3条数据，按照数据插入顺序
        text.first(3).print();
        System.out.println("==========================");
        // 获取分组后每组前2条数据
        text.groupBy(0).first(2).print();
        System.out.println("==========================");
        // 获取分组后排序的前2条
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();

        System.out.println("===========================");
        // 不分组，先按照第一个字段升序，在按照第二个字段降序，取前2条
        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();

    }
}
