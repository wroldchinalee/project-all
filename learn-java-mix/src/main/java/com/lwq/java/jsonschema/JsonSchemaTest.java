package com.lwq.java.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author: LWQ
 * @create: 2020/12/7
 * @description: JsonSchemaTest
 **/
public class JsonSchemaTest {
    public static void main(String[] args) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readValue(JsonSchemaConstant.SCHEMA, JsonNode.class);
        JsonNode typeNode = jsonNode.get("type");
        if (typeNode.getNodeType() == JsonNodeType.STRING) {
            System.out.println(typeNode.asText());
        }else{
            throw new Exception("第一个就不对！");
        }
    }

}
