package com.lwq.bigdata.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.*;

/**
 * Created by Administrator on 2020-12-06.
 */
public class BroadcastDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 18));
        broadData.add(new Tuple2<>("ls", 20));
        broadData.add(new Tuple2<>("ww", 17));
        DataSet<Tuple2<String, Integer>> tupleData =
                environment.fromCollection(broadData);

        // 将需要转换成map
        MapOperator<Tuple2<String, Integer>, Map<String, Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });

        List<String> list = Arrays.asList("zs", "ls", "ww");
        MapOperator<String, String> operator = environment.fromCollection(list).map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new
                    ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 3.获取广播变量
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadcast_map");
                for (HashMap<String, Integer> map : this.broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer number = allMap.getOrDefault(value, 0);
                return value + "_" + number;
            }
        }).withBroadcastSet(toBroadcast, "broadcast_map");

        operator.print();


    }
}
