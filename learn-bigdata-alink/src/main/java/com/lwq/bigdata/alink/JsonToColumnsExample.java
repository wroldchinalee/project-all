package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.batch.dataproc.JsonToColumnsBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.fastjson.JSON;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/11/23
 * @description: JsonToColumnsExample
 **/
public class JsonToColumnsExample {
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
        JsonToColumnsBatchOp jsonToColumnsBatchOp = inOp.link(new JsonToColumnsBatchOp()
                .setSelectedCol("message")
                .setSchemaStr("db string,table string,id int,name string"));
        List<Row> message = (List<Row>) jsonToColumnsBatchOp.collect();
        jsonToColumnsBatchOp.getOutputTable();
        int i = 1;
        for (Row row : message) {
            System.out.printf("第%d行数据:\n", i);
            System.out.println(row.getArity());
            for (int j = 0; j < row.getArity(); j++) {
                System.out.printf("第%d个数据:\n", j + 1);
                System.out.printf("类型:%s|", row.getField(j).getClass());
                System.out.printf("value:%s\n", row.getField(j));
            }
        }
    }
}
