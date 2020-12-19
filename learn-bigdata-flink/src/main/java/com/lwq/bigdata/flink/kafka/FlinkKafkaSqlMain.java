package com.lwq.bigdata.flink.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/18
 * @description: FlinkKafkaSqlMain
 **/
public class FlinkKafkaSqlMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new Kafka().version("0.10").topic("test")
                .startFromEarliest().property("zookeeper.connect", "192.168.233.130:2181/kafka")
                .property("bootstrap.servers", "192.168.233.130:9092"))
                .withSchema(new Schema()
                        .field("__db", DataTypes.STRING())
                        .field("data", DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING())))))
                .withFormat(new Json().failOnMissingField(true)
                        .schema(getType2())
//                        .jsonSchema(jsonSchema1)).inAppendMode()
                        .jsonSchema(jsonSchema2)).inAppendMode()
                .createTemporaryTable("amvcha");


        Table filterTable = tableEnv.sqlQuery("select __db,data[1].id from amvcha");
        DataStream<Row> resultStream = tableEnv.toAppendStream(filterTable, TypeInformation.of(Row.class));
        resultStream.print();
        env.execute("FlinkKafkaSqlMain");
    }

    public static String jsonSchema1 = "{\n" +
            "\ttype:'object',\n" +
            "\tproperties:{\n" +
            "\t\t__db:{type:'string'}\n" +
            "\t}\n" +
            "\n" +
            "}";

    public static String jsonSchema2 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"__db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\"items\": {\n" +
            "\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\"id\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "\n" +
            "}";

    public static TypeInformation<Row> getType1() {
        TypeInformation[] types = new TypeInformation[1]; // 3个字段
        String[] fieldNames = new String[1];

        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
//        types[1] = BasicTypeInfo.STRING_TYPE_INFO;
//        types[2] = BasicTypeInfo.DOUBLE_TYPE_INFO;

        fieldNames[0] = "__db";
//        fieldNames[1] = "id";
//        fieldNames[2] = "temp";

        return new RowTypeInfo(types, fieldNames);
    }

    public static TypeInformation<Row> getType2() {
        TypeInformation[] types = new TypeInformation[2]; // 3个字段
        String[] fieldNames = new String[2];

        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.STRING_TYPE_INFO;
//        types[2] = BasicTypeInfo.DOUBLE_TYPE_INFO;

        fieldNames[0] = "__db";
        fieldNames[1] = "id";
//        fieldNames[2] = "temp";

        return new RowTypeInfo(types, fieldNames);
    }
}
