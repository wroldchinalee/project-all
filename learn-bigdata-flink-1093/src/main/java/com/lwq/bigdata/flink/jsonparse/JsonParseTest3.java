package com.lwq.bigdata.flink.jsonparse;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import com.lwq.bigdata.flink.jsonparse.JsonParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author: LWQ
 * @create: 2020/12/23
 * @description: JsonParseTest3
 **/
public class JsonParseTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
//        DataStreamSource<String> streamSource = environment.readTextFile("E:\\source_code\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\input.txt");
        DataStreamSource<String> streamSource = environment.readTextFile("E:\\source_code\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\input2.txt");

        JsonParser jsonParser = new JsonParser.Builder(JsonSchemaHolder.JSON_SCHEMA4).build();
        SingleOutputStreamOperator<Row> resultStream = streamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                byte[] bytes = value.getBytes("UTF-8");
                Row row = jsonParser.deserialize(bytes);
                Object[] objs = ((Object[]) row.getField(1));
                for (Object obj : objs) {
                    System.out.println(obj.getClass());
                }
                RowTypeInfo rowTypeInfo = (RowTypeInfo) jsonParser.getProducedType();
                TypeInformation<Object> dataTypeInfo = rowTypeInfo.getTypeAt("data");
                System.out.printf("dataTypeInfo:%s\n", dataTypeInfo);
                ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) dataTypeInfo;
                System.out.printf("elementTypeInfo:%s\n", objectArrayTypeInfo.getComponentInfo());
                RowTypeInfo componentInfo = (RowTypeInfo) objectArrayTypeInfo.getComponentInfo();
                System.out.printf("componentInfo:%s\n", componentInfo);
                String[] fieldNames = componentInfo.getFieldNames();
                for (String fieldName : fieldNames) {
                    System.out.printf("fieldName:%s\n", fieldName);
                }
                System.out.println();
                TypeInformation<Object> dataTypeInfo2 = rowTypeInfo.getTypeAt("data2");
                System.out.printf("dataTypeInfo2:%s\n", dataTypeInfo2);
                BasicArrayTypeInfo objectArrayTypeInfo2 = (BasicArrayTypeInfo) dataTypeInfo2;
                System.out.printf("elementTypeInfo2:%s\n", objectArrayTypeInfo2.getComponentInfo());
//                RowTypeInfo componentInfo2 = (RowTypeInfo) objectArrayTypeInfo2.getComponentInfo();
//                System.out.printf("componentInfo2:%s\n",componentInfo2);


                return row;
            }
        });

        resultStream.print();
        environment.execute();
    }

    static class MyFlatMapFunction implements FlatMapFunction<String, Row> {
        JsonParser jsonParser = new JsonParser.Builder(JsonSchemaHolder.JSON_SCHEMA1).build();

        @Override
        public void flatMap(String value, Collector<Row> out) throws Exception {
            byte[] bytes = value.getBytes("UTF-8");
            Row row = jsonParser.deserialize(bytes);
            Object[] objs = ((Object[]) row.getField(1));
            for (Object obj : objs) {
                System.out.println(obj.getClass());
            }
        }
    }
}
