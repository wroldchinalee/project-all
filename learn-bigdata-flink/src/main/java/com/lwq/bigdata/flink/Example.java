package com.lwq.bigdata.flink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;

/**
 * Created by Administrator on 2020-11-25.
 */
public class Example {
    public static void main(String[] args) {
        LocalEnvironment localEnvironment = ExecutionEnvironment.createLocalEnvironment();

        String inputPath = "F:\\src\\tuling\\project-all\\learn-bigdata-flink\\src\\main\\resources\\sensor.txt";
        DataSource<String> inputSource = localEnvironment.readTextFile(inputPath);
        // 先转换类型
        MapOperator<String, Sensor> mapOperator = inputSource.map(new MapFunction<String, Sensor>() {
            public Sensor map(String s) throws Exception {
                String[] arr = s.split(",");
                Sensor sensor = new Sensor(arr[0], Long.parseLong(arr[1]), Double.parseDouble(arr[2]));
                return sensor;
            }
        });

    }
}


