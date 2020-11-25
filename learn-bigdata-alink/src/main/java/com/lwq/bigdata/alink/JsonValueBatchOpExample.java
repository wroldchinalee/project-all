package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.fastjson.JSON;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/11/23
 * @description: Json2ColumnsExample2
 * import numpy as np
 * import pandas as pd
 * data = np.array([
 * ["{a:boy,b:{b1:1,b2:2}}"],
 * ["{a:girl,b:{b1:1,b2:2}}"]])
 * df = pd.DataFrame({"str": data[:, 0]})
 * batchData = dataframeToOperator(df, schemaStr='str string', op_type='batch')
 * JsonValueBatchOp().setJsonPath(["$.a","$.b.b1"]).setSelectedCol("str").setOutputCols(["f0","f1"]).linkFrom(batchData).print()
 **/
public class JsonValueBatchOpExample {
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
        // 将source与数据处理操作链接
        List<Row> message = inOp.link(new JsonValueBatchOp()
                .setSkipFailed(true)
                // 通过message拿到json
                .setSelectedCol("message")
                // 输出的字段
                .setOutputCols(new String[]{"db", "table", "op", "data_id", "data_name"})
                // 上面输出字段在json中的路径
                .setJsonPath(new String[]{"$.db", "$.table", "$.op", "$.datas.id", "$.datas.name"})).collect();
        // 打印输出
        for (Row row : message) {
            System.out.println(row);
        }
    }
}

class KafkaSchema {
    private String db;
    private String table;
    private String op;
    private String position;
    private Map<String, String> datas;

    public KafkaSchema(String db, String table, String op, String position) {
        this.db = db;
        this.table = table;
        this.op = op;
        this.position = position;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getOp() {
        return op;
    }

    public String getPosition() {
        return position;
    }

    public Map<String, String> getDatas() {
        return datas;
    }

    public void setDatas(Map<String, String> datas) {
        this.datas = datas;
    }
}
