package com.lwq.bigdata.flink.tablesql;

import com.lwq.bigdata.flink.Sensor;
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
 * Created by Administrator on 2020-12-14.
 */
public class RowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> streamSource = environment.addSource(new RowSource(),getRowTypeInfo());
        // 1.基于env创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 2.基于tableEnv，将流转换成表
        Table table = tableEnvironment.fromDataStream(streamSource);
        DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(table, TypeInformation.of(Row.class));
//        rowDataStream.print();
        Table filterTable = table.select("id,temp").filter("id=='sensor_1'");

//        DataStream<Sensor> resultStream = tableEnvironment.toAppendStream(filterTable, TypeInformation.of(Sensor.class));
        DataStream<Row> resultStream = tableEnvironment.toAppendStream(filterTable, TypeInformation.of(Row.class));
        resultStream.print();
        environment.execute("example");

    }

    /**
     * 生成Row类型的TypeInformation.
     */
    private static RowTypeInfo getRowTypeInfo() {
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
