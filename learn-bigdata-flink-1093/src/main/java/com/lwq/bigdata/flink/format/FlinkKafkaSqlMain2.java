package com.lwq.bigdata.flink.format;

import com.lwq.bigdata.flink.format.function.CustomFlatMapFunction;
import com.lwq.bigdata.flink.format.function.CustomSplit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class FlinkKafkaSqlMain2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(
                // kafka连接配置
                new Kafka().version("0.10").topic("test2")
                        .startFromEarliest().property("zookeeper.connect", "192.168.233.130:2181/kafka")
                        .property("bootstrap.servers", "192.168.233.130:9092"))
                // 表schema定义
                .withSchema(getSchema())
                // kafka读取格式定义，这里读取json格式，并且在没有字段时抛异常
                .withFormat(new Json().failOnMissingField(true)
                        // json schema，可以自己指定，也可以通过表schema派生
                        .jsonSchema(JSON_SCHEMA_PARSE)).inAppendMode()
                .registerTableSource("amvcha");


        tableEnv.registerFunction("myflat",new CustomFlatMapFunction());
//        Table filterTable = tableEnv.sqlQuery("select __db,data[1].x,data[1].y,data[2].x,data[2].y,data[3].x,data[3].y from amvcha");
        Table filterTable = tableEnv.sqlQuery("select __db, x1,y1 from amvcha, LATERAL TABLE(myflat(data)) as T(x1,y1)");
//        Table filterTable = tableEnv.sqlQuery("select __db, x1,y1 from amvcha, LATERAL TABLE(data) as T(x1,y1)");
//        Table filterTable = tableEnv.sqlQuery("select __db,data.x,data.y from amvcha");
        DataStream<Row> resultStream = tableEnv.toAppendStream(filterTable, TypeInformation.of(Row.class));
        resultStream.print();
        env.execute("FlinkKafkaSqlMain");
    }

    public static Schema getSchema() {
        return new Schema()
                .field("__db", Types.STRING)
                .field("data", Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{"x", "y"}, Types.STRING, Types.STRING)));
    }

    public static String JSON_SCHEMA_PARSE = "{\n" +
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
            "\t\t\t\t\t\"x\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t},\n" +
            "\t\t\t\t\t\"y\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "\n" +
            "}";
}
