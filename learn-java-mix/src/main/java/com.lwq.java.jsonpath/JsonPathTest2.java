package com.lwq.java.jsonpath;

import com.jayway.jsonpath.JsonPath;

/**
 * @author: LWQ
 * @create: 2020/12/9
 * @description: JsonPathTest2
 **/
public class JsonPathTest2 {
    public static final String JSON = "{\n" +
            "\t\"__db\": \"dcbsdb\",\n" +
            "\t\"__table\": \"amvcha\",\n" +
            "\t\"__type\": \"update\",\n" +
            "\t\"data\": [{\n" +
            "\t\t\t\"bankNum\": \"001\",\n" +
            "\t\t\t\"col2\": \"abc\"\n" +
            "\n" +
            "\t\t},\n" +
            "\t\t{\n" +
            "\t\t\t\"bankNum\": \"002\",\n" +
            "\t\t\t\"col2\": \"def\"\n" +
            "\n" +
            "\t\t}\n" +
            "\n" +
            "\t]\n" +
            "\n" +
            "}";

    public static void main(String[] args) {
        String __db = JsonPath.read(JSON, "$.__db");
        System.out.println(JsonPath.read(JSON, "$.__db").getClass());
        System.out.printf("__db:%s\n", __db);
        Object read = JsonPath.read(JSON, "$.data");
        System.out.println(read.getClass());
        Object read2 = JsonPath.read(JSON, "$.data[0]");
        System.out.println(read2.getClass());
        Object read3 = JsonPath.read(JSON, "$.data[*]");
        System.out.println(read3.getClass());
    }

}
