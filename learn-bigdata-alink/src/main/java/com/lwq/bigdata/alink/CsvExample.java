package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.CsvToJsonBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/11/26
 * @description: CsvExample
 **/
public class CsvExample {
    public static void main(String[] args) throws Exception {
        Row[] rows = new Row[2];
        Row row1 = new Row(2);
        row1.setField(0, "1");
        row1.setField(1, "1.0,2.0");
        rows[0] = row1;
        Row row2 = new Row(2);
        row2.setField(0, "2");
        row2.setField(1, "4.0,8.0");
        rows[1] = row2;
        String[] colNames = {"f0", "f1"};
        MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(rows, colNames);

        List collect = sourceBatchOp.link(new CsvToJsonBatchOp()
                .setCsvCol("csv")
                .setSchemaStr("f0 double,f1 double")
                .setJsonCol("json")).collect();
        for (Object o : collect) {
            System.out.println(o);
        }
    }
}
