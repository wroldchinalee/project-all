package com.lwq.bigdata.flink.stream.transform;

import com.lwq.bigdata.flink.tablesql.RowSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: SqlMain
 **/
public class SqlMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> streamSource = environment.addSource(new RowSource());

        String sql = "select id,temp from t_sensor where id='sensor_1'";
        Config config = new Config(sql);
        SqlTransformer sqlTransformer = new SqlTransformer(config, environment);
        ArrayList<MetaColumn> metaColumns = new ArrayList<>();
        MetaColumn metaColumn = new MetaColumn("id,", "string");
        metaColumns.add(metaColumn);
        MetaColumn metaColumn2 = new MetaColumn("timestamp", "long");
        metaColumns.add(metaColumn2);
        MetaColumn metaColumn3 = new MetaColumn("temp", "double ");
        metaColumns.add(metaColumn3);
        sqlTransformer.setMetaColumns(metaColumns);

        DataStream<Row> resultStream = sqlTransformer.transform(streamSource);
        resultStream.print("sql result");

        environment.execute("sql example");

    }
}
