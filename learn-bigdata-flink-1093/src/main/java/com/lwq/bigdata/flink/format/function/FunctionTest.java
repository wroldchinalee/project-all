package com.lwq.bigdata.flink.format.function;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author: LWQ
 * @create: 2020/12/20
 * @description: FunctionTest
 **/
public class FunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("split", new CustomSplit());
        DataStreamSource<String> streamSource = env.readTextFile("E:\\source_code\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\split.txt");

        Table table = tableEnv.fromDataStream(streamSource, "word");
        tableEnv.registerTable("test", table);
        Table table2 = tableEnv.sqlQuery("select word,word1 from test, LATERAL TABLE(split(word)) as T(word1)");

        tableEnv.toAppendStream(table2, Types.ROW(Types.STRING, Types.STRING)).print();
        env.execute("test");


    }
}
