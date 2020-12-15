package com.lwq.java.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/12/7
 * @description: JsonLeaf
 **/
public class JsonLeaf {
    public static void jsonLeaf(JsonNode node) {
        if (node.isValueNode()) {
            System.out.println(node.toString());
            return;
        }

        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                jsonLeaf(entry.getValue());
            }
        }

        if (node.isArray()) {
            Iterator<JsonNode> it = node.iterator();
            while (it.hasNext()) {
                jsonLeaf(it.next());
            }
        }
    }

    public static void main(String[] args) {
        try {
            String json = JsonSchemaConstant.SCHEMA;
            ObjectMapper jackson = new ObjectMapper();
            JsonNode node = jackson.readTree(json);
            jsonLeaf(node);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
