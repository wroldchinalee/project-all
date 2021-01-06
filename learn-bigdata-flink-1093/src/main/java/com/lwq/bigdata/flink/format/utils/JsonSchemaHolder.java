package com.lwq.bigdata.flink.format.utils;

/**
 * @author: LWQ
 * @create: 2020/12/21
 * @description: JsonSchemaHolder
 **/
public class JsonSchemaHolder {
    public static final String JSON_SCHEMA1 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"__db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\"items\": {\n" +
            "\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\"properties\": {\n" +
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
            "\n" +
            "}";

    public static final String JSON_SCHEMA2 = "{\n" +
            "\t\"data\": {\n" +
            "\t\t\"type\": \"array\",\n" +
            "\t\t\"items\": {\n" +
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

    public static final String JSON_SCHEMA3 = "{\n" +
            "\t\"__db\": {\n" +
            "\t\t\"type\": \"string\"\n" +
            "\t}\n" +
            "}";

    public static final String JSON_SCHEMA4 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"__db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\"properties\": {\n" +
            "\t\t\t\t\"aa\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t\"bb\": {\n" +
            "\t\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\t\"cc\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\"d\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\t\t\t\"ee\": {\n" +
            "\t\t\t\t\t\t\t\t\t\"type\": \"integer\"\n" +
            "\t\t\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\t\t\"ff\": {\n" +
            "\t\t\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";

    public static final String JSON_SCHEMA5 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"__db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\"properties\": {\n" +
            "\t\t\t\t\"aa\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t\"bb\": {\n" +
            "\t\t\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\t\t\"items\": {\n" +
            "\t\t\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\t\t\"ee\": {\n" +
            "\t\t\t\t\t\t\t\t\"type\": \"integer\"\n" +
            "\t\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\t\"ff\": {\n" +
            "\t\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";

    public static final String JSON_SCHEMA6 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"__db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\"items\": {\n" +
            "\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\"x\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t},\n" +
            "\t\t\t\t\t\"y\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t},\n" +
            "\t\t\"data2\": {\n" +
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

    public static final String JSON_SCHEMA7 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"aa\": {\n" +
            "\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\"properties\": {\n" +
            "\t\t\t\t\"bb\": {\n" +
            "\t\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\t\"cc\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"number\"\n" +
            "\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\"dd\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\"ff\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\t\t\t\t\"items\": {\n" +
            "\t\t\t\t\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\t\t\t\t\"gg\": {\n" +
            "\t\t\t\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\t\t\t\"hh\": {\n" +
            "\t\t\t\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t\"ee\": {\n" +
            "\t\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\t\"jj\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t},\n" +
            "\t\t\t\t\t\t\"kk\": {\n" +
            "\t\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t\t}\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t\"ll\": {\n" +
            "\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t},\n" +
            "\t\t\"ii\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"mm\": {\n" +
            "\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\"items\": {\n" +
            "\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";
}
