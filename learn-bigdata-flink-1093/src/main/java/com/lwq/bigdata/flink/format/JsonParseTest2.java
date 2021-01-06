package com.lwq.bigdata.flink.format;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/21
 * @description: JsonParseTest2
 **/
public class JsonParseTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> streamSource = environment.readTextFile("E:\\source_code\\project-all\\learn-bigdata-flink-1093\\src\\main\\resources\\input.txt");
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        SingleOutputStreamOperator<Row> resultStream = streamSource.map(new JsonDeserFunction());


//        Table table = tableEnvironment.sqlQuery("select id,temp from t_sensor where id='sensor_1'");
//        DataStream<Row> resultStream = tableEnvironment.toAppendStream(table, TypeInformation.of(Row.class));
        resultStream.print();
        environment.execute("example");
    }
}

class JsonDeserFunction implements MapFunction<String, Row> {
    public static volatile int count = 0;

    @Override
    public Row map(String value) throws Exception {
        byte[] bytes = value.getBytes("UTF-8");
        JsonRowDeserializationSchema deserSchema = new JsonRowDeserializationSchema.Builder(JsonSchemaHolder.JSON_SCHEMA1).build();
        TypeInformation<Row> producedType = deserSchema.getProducedType();
        if (count == 0) {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) producedType;
            System.out.println(producedType);
            System.out.println("-------------fieldName--------------");
            String[] fieldNames = rowTypeInfo.getFieldNames();
            for (String fieldName : fieldNames) {
                System.out.printf("field:%s\t", fieldName);
            }
            System.out.println();
            System.out.println("-------------fieldType---------------");
            TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
            for (TypeInformation<?> fieldType : fieldTypes) {
                System.out.printf("fieldType:%s\n", fieldType);
                System.out.println(fieldType.getTypeClass());
                if (fieldType.equals(Types.OBJECT_ARRAY(Types.ROW()))) {
                }
            }

//            System.out.println("-----------------flat---------------------");
//            ArrayList<CompositeType.FlatFieldDescriptor> list = new ArrayList<>();
//            rowTypeInfo.getFlatFields("data", 0, list);
//            for (CompositeType.FlatFieldDescriptor flatFieldDescriptor : list) {
//                System.out.println(flatFieldDescriptor);
//            }
            System.out.println("-----------------------------------");
            count++;

        }
        Row row = deserSchema.deserialize(bytes);

        int arity = row.getArity();
        for (int i = 0; i < arity; i++) {
            Object field = row.getField(i);
            System.out.println(field.getClass());
            if(field instanceof Object[]){
                Object[] objArray = (Object[]) field;
                for (int j = 0; j < objArray.length; j++) {
                    Row tempRow = (Row) objArray[j];
                    System.out.println(tempRow);
                }
            }
        }

        System.out.println(row);

        return row;
    }
}
