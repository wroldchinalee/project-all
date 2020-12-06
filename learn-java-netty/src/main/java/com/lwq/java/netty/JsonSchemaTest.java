package com.lwq.java.netty;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.schema.JsonSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2020-12-03.
 */
public class JsonSchemaTest {
    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationConfig.Feature.WRITE_ENUMS_USING_TO_STRING, true);
        JsonSchema schema = objectMapper.generateJsonSchema(KafkaTopic.class);
        String schemaStr = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
        System.out.println(schemaStr);

    }
}

class KafkaTopic {
    private String __db;
    private String __table;
    private List<Map<String, String>> data = new ArrayList<Map<String, String>>();

    public KafkaTopic() {
    }

    public String get__db() {
        return __db;
    }

    public void set__db(String __db) {
        this.__db = __db;
    }

    public String get__table() {
        return __table;
    }

    public void set__table(String __table) {
        this.__table = __table;
    }

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }
}

