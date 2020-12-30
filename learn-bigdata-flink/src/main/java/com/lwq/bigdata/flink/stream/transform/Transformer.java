package com.lwq.bigdata.flink.stream.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * @author: LWQ
 * @create: 2020/12/15
 * @description: Transformer
 **/
public class Transformer implements Serializable  {
    public String filterRule = "";
    private transient StreamExecutionEnvironment env;

    public Transformer(String filterRule, StreamExecutionEnvironment env) {
        this.filterRule = filterRule;
        this.env = env;
    }

    public DataStream<Row> transform(DataStream<Row> dataStream) {
        SingleOutputStreamOperator<Row> resultStream = dataStream.filter(new RichFilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                if (StringUtils.isEmpty(filterRule)) {
                    return true;
                }
                return false;
            }
        });
        return resultStream;
    }
}
