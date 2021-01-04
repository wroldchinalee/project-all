package com.lwq.bigdata.flink.jsonparse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author: LWQ
 * @create: 2020/12/28
 * @description: JsonParseExample1
 **/
public class JsonParseExample1 {
    public static void main(String[] args) throws IOException {
        JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(SCHEMA1).failOnMissingField().build();
        Row row = deserializationSchema.deserialize(INPUT1.getBytes("UTF-8"));
        System.out.println(row);
        RowTypeInfo producedType = ((RowTypeInfo) deserializationSchema.getProducedType());
        String[] fieldNames = producedType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            System.out.printf("%s,", fieldNames[i]);
        }
    }

    static String INPUT1 = "{\n" +
            "\t\"schema\": \"dcbsdb\",\n" +
            "\t\"table\": \"xy\",\n" +
            "\t\"eventType\": \"update\",\n" +
            "\t\"before\": {\n" +
            "\t\t\"x\": \"1\",\n" +
            "\t\t\"y\": \"3\"\n" +
            "\t},\n" +
            "\t\"after\": {\n" +
            "\t\t\"x\": \"4\",\n" +
            "\t\t\"y\": \"5\"\n" +
            "\t}\n" +
            "}";
    static String SCHEMA1 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"schema\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"table\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"eventType\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"before\": {\n" +
            "\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\"properties\": {\n" +
            "\t\t\t\t\"x\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t\"y\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t},\n" +
            "\t\t\"after\": {\n" +
            "\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\"properties\": {\n" +
            "\t\t\t\t\"x\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t\"y\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";
}
