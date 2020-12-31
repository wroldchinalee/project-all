package com.lwq.bigdata.flink;

/**
 * Created by Administrator on 2020-12-22.
 */
public class JsonDoc {
    public static final String JSON_DATA1 = "{\n" +
            "\t\"db\": \"dcbsdb\",\n" +
            "\t\"table\": \"amvcha\",\n" +
            "\t\"data\": [{\n" +
            "\t\t\t\"id\": 1,\n" +
            "\t\t\t\"name\": \"aa\"\n" +
            "\t\t},\n" +
            "\t\t{\n" +
            "\t\t\t\"id\": 2,\n" +
            "\t\t\t\"name\": \"bb\"\n" +
            "\t\t},\n" +
            "\t\t{\n" +
            "\t\t\t\"id\": 3,\n" +
            "\t\t\t\"name\": \"cc\"\n" +
            "\t\t}\n" +
            "\t]\n" +
            "}";

    public static final String JSON_SCHEMA1 = "{\n" +
            "\t\"type\": \"object\",\n" +
            "\t\"properties\": {\n" +
            "\t\t\"db\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"table\": {\n" +
            "\t\t\t\"type\": \"string\"\n" +
            "\t\t},\n" +
            "\t\t\"data\": {\n" +
            "\t\t\t\"type\": \"array\",\n" +
            "\t\t\t\"items\": {\n" +
            "\t\t\t\t\"type\": \"object\",\n" +
            "\t\t\t\t\"properties\": {\n" +
            "\t\t\t\t\t\"id\": {\n" +
            "\t\t\t\t\t\t\"type\": \"integer\"\n" +
            "\t\t\t\t\t},\n" +
            "\t\t\t\t\t\"name\": {\n" +
            "\t\t\t\t\t\t\"type\": \"string\"\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t}\n" +
            "\t\t\t}\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}";

}
