package com.lwq.bigdata.flink.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/20
 * @description: FlinkKafkaDemo
 **/
public class FlinkKafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new Kafka() //kafka连接配置
                .version("0.10")
                .topic("test")
                .startFromEarliest()
                .property("group.id", "g1")
                .property("bootstrap.servers", "192.168.233.130:9092")
        ).withFormat(new Json()
                .failOnMissingField(true)
                .deriveSchema()
        ).withSchema(new Schema()
                .field("__db", Types.STRING)
                .field("data", Types.OBJECT_ARRAY(
                        Types.ROW_NAMED(new String[]{"id"},
                                Types.STRING)
                        )
                )
        ).inAppendMode().registerTableSource("amvcha");
//        Table table = tableEnv.sqlQuery("select __db,data[1].id from amvcha");
//        tableEnv.toAppendStream(table, getType()).print();
        Table table = tableEnv.sqlQuery("select __db,data[1].id,data from amvcha");
        tableEnv.toAppendStream(table, getType2()).print();
        env.execute("FlinkKafkaDemo");
    }


    public static TypeInformation<Row> getType2() {
        String[] fields = new String[]{"__db", "id", "data"};
        TypeInformation dataType = Types.OBJECT_ARRAY(
                Types.ROW_NAMED(new String[]{"id"},
                        Types.STRING)
        );
        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, dataType};
        return Types.ROW_NAMED(fields, types);
    }

    public static TypeInformation<Row> getType() {

        TypeInformation[] types = new TypeInformation[2]; // 2个字段
        String[] fieldNames = new String[2];

        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.STRING_TYPE_INFO;

        fieldNames[0] = "__db";
        fieldNames[1] = "id";

        return new RowTypeInfo(types, fieldNames);
    }

}
