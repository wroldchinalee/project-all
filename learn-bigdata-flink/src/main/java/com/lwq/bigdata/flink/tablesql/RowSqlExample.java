package com.lwq.bigdata.flink.tablesql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: RowSqlExample
 **/
public class RowSqlExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> streamSource = environment.addSource(new RowSource(), getRowTypeInfo());
        // 1.基于env创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 2.基于tableEnv，将流转换成表
//        Table table = tableEnvironment.fromDataStream(streamSource);
//        tableEnvironment.createTemporaryView("t_sensor", streamSource);
        tableEnvironment.createTemporaryView("t_sensor", streamSource);
        Table table = tableEnvironment.sqlQuery("select id,temp from t_sensor where id='sensor_1'");
        DataStream<Row> resultStream = tableEnvironment.toAppendStream(table, TypeInformation.of(Row.class));
        resultStream.print();
        environment.execute("example");

    }

    public static RowTypeInfo getRowTypeInfo() {
        TypeInformation[] types = new TypeInformation[3]; // 3个字段
        String[] fieldNames = new String[3];

        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.LONG_TYPE_INFO;
        types[2] = BasicTypeInfo.DOUBLE_TYPE_INFO;

        fieldNames[0] = "id";
        fieldNames[1] = "timestamp";
        fieldNames[2] = "temp";

        return new RowTypeInfo(types, fieldNames);
    }
}
