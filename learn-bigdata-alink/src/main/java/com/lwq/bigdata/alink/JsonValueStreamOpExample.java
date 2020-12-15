package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/11/27
 * @description: JsonValueStreamOpExample
 **/
public class JsonValueStreamOpExample {

    public static void main(String[] args) throws Exception {
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
        MemSourceStreamOp sourceStreamOp = new MemSourceStreamOp(jsonArr, "json");
        JsonValueStreamOp jsonValueStreamOp = new JsonValueStreamOp().setSkipFailed(true)
                // 通过message拿到json
                .setSelectedCol("json")
                // 输出的字段
                .setOutputCols(new String[]{"db", "table", "op", "data_id", "data_name"})
                // 上面输出字段在json中的路径
                .setJsonPath(new String[]{"$.db", "$.table", "$.op", "$.datas.id", "$.datas.name"});
        jsonValueStreamOp.linkFrom(sourceStreamOp).print();
        StreamOperator.execute();


    }
}
