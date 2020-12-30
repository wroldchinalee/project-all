package com.lwq.bigdata.flink.stream.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: SqlTransformer
 **/
public class SqlTransformer implements Serializable {
    private transient StreamExecutionEnvironment env;
    private transient StreamTableEnvironment tableEnv;
    private List<MetaColumn> metaColumns;

    public SqlTransformer(Config config, StreamExecutionEnvironment env) {
        this.env = env;
        this.tableEnv = StreamTableEnvironment.create(env);
    }

    public DataStream<Row> transform(DataStream<Row> dataStream) {
        SingleOutputStreamOperator<Row> typeStream = dataStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                return value;
            }
        }).returns(getRowTypeInfo());

        tableEnv.createTemporaryView("t_sensor", typeStream);
        Table table = tableEnv.sqlQuery("select id,temp from t_sensor where id='sensor_1'");

        DataStream<Row> resultStream = tableEnv.toAppendStream(table, TypeInformation.of(Row.class));
        return resultStream;
    }

    public List<MetaColumn> getMetaColumns() {
        return metaColumns;
    }

    public void setMetaColumns(List<MetaColumn> metaColumns) {
        this.metaColumns = metaColumns;
    }

    public RowTypeInfo getRowTypeInfo() {
        TypeInformation[] types = new TypeInformation[metaColumns.size()]; // 3个字段
        String[] fieldNames = new String[metaColumns.size()];

        int index = 0;
        for (MetaColumn metaColumn : metaColumns) {
            fieldNames[index] = metaColumn.getName();
            TypeInformation typeInformation = FlinkTypeUtil.convertType(metaColumn.getType());
            types[index] = typeInformation;
        }
        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.LONG_TYPE_INFO;
        types[2] = BasicTypeInfo.DOUBLE_TYPE_INFO;

        fieldNames[0] = "id";
        fieldNames[1] = "timestamp";
        fieldNames[2] = "temp";
        return new RowTypeInfo(types, fieldNames);
    }
}
