package com.lwq.bigdata.flink.tablesql;

import com.lwq.bigdata.flink.Sensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by Administrator on 2020-12-14.
 */
public class SensorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sensor> streamSource = environment.addSource(new SensorSource());
        // 1.基于env创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 2.基于tableEnv，将流转换成表
        Table table = tableEnvironment.fromDataStream(streamSource);
//        tableEnvironment.createTemporaryView("sensor", mapStream);
        Table filterTable = table.select("id,temp").filter("id=='sensor_1'");

//        DataStream<Sensor> resultStream = tableEnvironment.toAppendStream(filterTable, TypeInformation.of(Sensor.class));
        DataStream<Row> resultStream = tableEnvironment.toAppendStream(filterTable, TypeInformation.of(Row.class));
        resultStream.print();
        environment.execute("example");

    }
}
