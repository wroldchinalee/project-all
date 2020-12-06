package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by Administrator on 2020-12-06.
 */
public class MapPartitionDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> list = new ArrayList<>();
        list.add("hello");
        list.add("hadoop");
        list.add("flink");

        DataSource<String> streamSource = environment.fromCollection(list);
        MapPartitionOperator<String, String> mapPartitionOperator = streamSource.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                // 获取数据库连接，注意，此时是一个分区的数据获取一次连接
                // values中保存了一个分区的数据
                Iterator<String> iterator = values.iterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    out.collect(next);
                }
            }
            // 关闭连接
        });

        mapPartitionOperator.print();
    }
}
