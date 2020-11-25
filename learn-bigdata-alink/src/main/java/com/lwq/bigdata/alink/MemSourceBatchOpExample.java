package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/11/25
 * @description: MemSourceBatchOpExample
 **/
public class MemSourceBatchOpExample {
    public static void main(String[] args) {
        // 先构建一个json
        String db = "dcbsdb";
        String table = "amvcha";
        String op = "UPDATE";
        String position = "mysql-bin.000001";
        KafkaSchema kafkaSchema = new KafkaSchema(db, table, op, position);
        Map<String, String> datas = new HashMap<String, String>();
        datas.put("id", "1");
        datas.put("name", "lwq");
        kafkaSchema.setDatas(datas);
        String json = JSON.toJSONString(kafkaSchema);

        String[] jsonArr = {json};
        // 创建一个内存source，将json传进去，json的colName是message
        MemSourceBatchOp inOp = new MemSourceBatchOp(jsonArr, "message");
        int length = inOp.getColNames().length;
        for (int i = 0; i < length; i++) {
            System.out.printf("列名:%s  类型:%s\n", inOp.getColNames()[i], inOp.getColTypes()[i]);
        }
    }
}
