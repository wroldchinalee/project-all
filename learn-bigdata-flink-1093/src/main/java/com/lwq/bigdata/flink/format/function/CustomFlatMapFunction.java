package com.lwq.bigdata.flink.format.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/20
 * @description: CustomFlatMapFunction
 **/
public class CustomFlatMapFunction extends TableFunction<Row> {

    public void eval(Row[] rows) {
        if (rows == null) {
            return;
        }
        for (Row row : rows) {
            collector.collect(row);
        }
    }


    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.STRING);
    }
}
