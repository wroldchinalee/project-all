package com.lwq.bigdata.flink.jsonparse;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @author: LWQ
 * @create: 2020/12/28
 * @description: JsonParseExample1
 **/
public class JsonParseExample2 {
    public static void main(String[] args) throws IOException {
        JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(SCHEMA2).build();
        Row row = deserializationSchema.deserialize(INPUT2.getBytes("UTF-8"));
        System.out.println(row);
        RowTypeInfo producedType = ((RowTypeInfo) deserializationSchema.getProducedType());
        String[] fieldNames = producedType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            System.out.printf("%s,", fieldNames[i]);
        }
    }

    static String INPUT2 = "{\n" +
            "\t\"__db\": \"dcbsdb\",\n" +
            "\t\"__table\": \"xy\",\n" +
            "\t\"__op\": \"U\",\n" +
            "\t\"data\": [{\n" +
            "\t\t\"before_x\": \"1\",\n" +
            "\t\t\"before_y\": \"3\",\n" +
            "\t\t\"x\": \"3\",\n" +
            "\t\t\"y\": \"3\"\n" +
            "\t}, {\n" +
            "\t\t\"before_x\": \"3\",\n" +
            "\t\t\"before_y\": \"4\",\n" +
            "\t\t\"x\": \"8\",\n" +
            "\t\t\"y\": \"10\"\n" +
            "\t}]\n" +
            "}";
    static String SCHEMA2 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"__db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"__table\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"__op\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\"items\": {\n" +
            "\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\"before_x\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t},\n" +
            "\t\t\t\t\t\"before_y\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t},\n" +
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
            "}";
}
