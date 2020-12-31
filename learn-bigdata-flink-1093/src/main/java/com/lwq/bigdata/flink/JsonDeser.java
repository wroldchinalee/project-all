package com.lwq.bigdata.flink;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Created by Administrator on 2020-12-22.
 */
public class JsonDeser {
    static String str = "";

    public static void main(String[] args) throws IOException {
        JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(JsonDoc.JSON_SCHEMA1).build();
        Row row = deserializationSchema.deserialize(JsonDoc.JSON_DATA1.getBytes("UTF-8"));
        System.out.println(row);
    }
}
