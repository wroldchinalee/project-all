package com.lwq.bigdata.alink;

import com.alibaba.alink.operator.batch.dataproc.CsvToColumnsBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.CsvToColumns;
import com.alibaba.alink.pipeline.dataproc.format.ColumnsToJson;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/11/26
 * @description: FormatTransExample
 **/
public class FormatTransExample {
    public static void main(String[] args) {
        //"row", "json", "vec", "kv", "csv", "f0", "f1"
//        csvToCols();
        colsToJson();
    }


    public static void csvToCols() {
        String[] data = DataConstants.getCsvData();

        MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(data, "row");
        List<Row> rows = sourceBatchOp.link(new CsvToColumnsBatchOp()
                .setSelectedCol("row")
                .setSchemaStr("f0 double,f1 double")).collect();
        for (int i = 0; i < rows.size(); i++) {
            System.out.println(rows.get(i));
        }
    }

    public static void colsToJson() {
        Row[] data = DataConstants.getColumsData();
        LocalEnvironment localEnvironment = ExecutionEnvironment.createLocalEnvironment();
        DataSource<Row> dataSource = localEnvironment.fromCollection(Arrays.asList(data));


        MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(data, "row");
    }
}


