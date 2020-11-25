package com.lwq.java.jsonpath;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.lwq.java.jsonpath.JsonRead;

/**
 * @author: LWQ
 * @create: 2020/11/25
 * @description: MultiReadFromDocument
 **/
public class MultiReadFromDocument {
    public static void main(String[] args) {
        String json = JsonRead.JSON;
        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
        String author0 = JsonPath.read(document, "$.store.book[0].author");
        String author1 = JsonPath.read(document, "$.store.book[1].author");
        System.out.printf("author0:%s\n", author0);
        System.out.printf("author1:%s", author1);
    }
}
