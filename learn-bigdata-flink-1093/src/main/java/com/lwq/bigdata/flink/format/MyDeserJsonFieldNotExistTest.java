package com.lwq.bigdata.flink.format;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.io.IOException;

import static com.lwq.bigdata.flink.format.utils.DeserializationSchemaMatcher.whenDeserializedWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

/**
 * @author: LWQ
 * @create: 2020/12/21
 * @description: MyDeserJsonFieldNotExistTest
 * 使用flink-json解析字段不存在情况
 **/
public class MyDeserJsonFieldNotExistTest {
    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("id", 123123123);
        // json数据只有一个字段id，类型为int，序列化
        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        // schema的字段为name，类型为string
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(
                new String[]{"name"},
                Types.STRING);


        JsonRowDeserializationSchema deserializationSchema =
                new JsonRowDeserializationSchema.Builder(rowTypeInformation)
                        .build();


        Row row1 = null;
        // 默认情况failOnMissingField参数为false，当json数据中没有schema需要的字段时
        // 会忽略缺少的字段
        try {
            row1 = deserializationSchema.deserialize(serializedJson);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(row1);


        // 将failOnMissingField参数设置为true,如果没有字段直接报错
        deserializationSchema = new JsonRowDeserializationSchema.Builder(rowTypeInformation)
                .failOnMissingField()
                .build();
        Row row2 = null;
        try {
            row2 = deserializationSchema.deserialize(serializedJson);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(row2);

    }
}
