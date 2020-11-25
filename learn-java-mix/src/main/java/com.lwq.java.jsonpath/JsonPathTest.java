package com.lwq.java.jsonpath;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import java.util.Date;
import java.util.List;

import static com.jayway.jsonpath.JsonPath.using;


/**
 * @author: LWQ
 * @create: 2020/11/24
 * @description: JsonPathTest
 **/
public class JsonPathTest {
    public static void main(String[] args) {
        String json = JsonRead.JSON;

        readValue(json);
        cast(json);
        mapping();
    }

    public static void readValue(String json) {
        List<String> authors = JsonPath.read(json, "$.store.book[*].author");
        for (int i = 0; i < authors.size(); i++) {
            System.out.println(authors.get(i));
        }
        System.out.println("====================readValue=====================");
    }

    public static void cast(String json) {
        // 正常
        String author = JsonPath.parse(json).read("$.store.book[0].author");
        System.out.printf("$.store.book[0].author:%s\n", author);
        // 抛出 java.lang.ClassCastException 异常
        try {
            List<String> list = JsonPath.parse(json).read("$.store.book[0].author");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("====================cast=====================");
    }

    public static void mapping() {
        String json = "{\"date_as_long\" : 1411455611975}";
        Date date = JsonPath.parse(json).read("$['date_as_long']", Date.class);
        System.out.println(date);
    }

    public static void path(String json){
        Configuration conf = Configuration.builder()
                .options(Option.AS_PATH_LIST).build();

        List<String> pathList = using(conf).parse(json).read("$..author");

    }
}
