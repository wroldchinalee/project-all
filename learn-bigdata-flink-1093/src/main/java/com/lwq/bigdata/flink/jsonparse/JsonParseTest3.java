package com.lwq.bigdata.flink.jsonparse;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * @author: LWQ
 * @create: 2020/12/23
 * @description: JsonParseTest3
 **/
public class JsonParseTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> streamSource = environment.readTextFile("E:\\source_code\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\input4.txt");

        JsonParser jsonParser = new JsonParser.Builder(JsonSchemaHolder.JSON_SCHEMA7).build();
        RowTypeInfo producedType = ((RowTypeInfo) jsonParser.getProducedType());
        System.out.println("productType:" + producedType);
        System.out.println(Arrays.toString(producedType.getFieldNames()));
        SingleOutputStreamOperator<Row> resultStream = streamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                byte[] bytes = value.getBytes("UTF-8");
                Row row = jsonParser.deserialize(bytes);
                int arity = row.getArity();
                System.out.printf("arity:%d\n", arity);
                for (int i = 0; i < arity; i++) {
                    System.out.printf("%s ", row.getField(i));
                }
                return row;
            }
        });

        resultStream.print();
        environment.execute();
    }
}
