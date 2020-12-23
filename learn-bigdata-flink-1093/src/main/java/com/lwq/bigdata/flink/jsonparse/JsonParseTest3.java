package com.lwq.bigdata.flink.jsonparse;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import com.lwq.bigdata.flink.jsonparse.JsonParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/23
 * @description: JsonParseTest3
 **/
public class JsonParseTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> streamSource = environment.readTextFile("E:\\source_code\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\input.txt");

        JsonParser jsonParser = new JsonParser.Builder(JsonSchemaHolder.JSON_SCHEMA1).build();
        SingleOutputStreamOperator<Row> resultStream = streamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                byte[] bytes = value.getBytes("UTF-8");
                Row row = jsonParser.deserialize(bytes);
                return row;
            }
        });

        resultStream.print();
        environment.execute();
    }
}
