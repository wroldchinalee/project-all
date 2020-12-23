package com.lwq.bigdata.flink.format;

import com.lwq.bigdata.flink.format.utils.DeserializationSchemaMatcher;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author: LWQ
 * @create: 2020/12/20
 * @description: JsonParseTest
 **/
public class JsonParseTest {
    public static String JSON_SCHEMA = "{" +
            "    type: 'object'," +
            "    properties: {" +
            "         id: { type: 'integer' }," +
            "         idOrNull: { type: ['integer', 'null'] }," +
            "         name: { type: 'string' }," +
            "         date: { type: 'string', format: 'date' }," +
            "         time: { type: 'string', format: 'time' }," +
            "         timestamp: { type: 'string', format: 'date-time' }," +
            "         bytes: { type: 'string', contentEncoding: 'base64' }," +
            "         numbers: { type: 'array', items: { type: 'integer' } }," +
            "         strings: { type: 'array', items: { type: 'string' } }," +
            "         nested: { " +
            "             type: 'object'," +
            "             properties: { " +
            "                 booleanField: { type: 'boolean' }," +
            "                 decimalField: { type: 'number' }" +
            "             }" +
            "         }" +
            "    }" +
            "}";

    public static void main(String[] args) throws IOException {
        String str = "x:{%s}";
        System.out.println(String.format(str, 1));


        final BigDecimal id = BigDecimal.valueOf(1238123899121L);
        final String name = "asdlkjasjkdla998y1122";
        final byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        final ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", id.longValue());
        root.putNull("idOrNull");
        root.put("name", name);
        root.put("date", "1990-10-14");
        root.put("time", "12:12:43Z");
        root.put("timestamp", "1990-10-14T12:12:43Z");
        root.put("bytes", bytes);
        root.putArray("numbers").add(1).add(2).add(3);
        root.putArray("strings").add("one").add("two").add("three");
        root.putObject("nested").put("booleanField", true).put("decimalField", 12);

        System.out.println(JSON_SCHEMA);
        final byte[] serializedJson = objectMapper.writeValueAsBytes(root);
        JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(JSON_SCHEMA).failOnMissingField().build();
        Row row = deserializationSchema.deserialize(serializedJson);
        System.out.println(row);
    }
}
