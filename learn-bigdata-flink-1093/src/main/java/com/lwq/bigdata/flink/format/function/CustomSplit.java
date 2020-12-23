package com.lwq.bigdata.flink.format.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/12/20
 * @description: CustomSplit
 **/
public class CustomSplit extends TableFunction<String> {

    //    public void eval(String str) {
//        for (String s : str.split(" ")) {
//            Row row = new Row(2);
//            row.setField(0, s);
//            row.setField(1, s.length());
//            collect(row);
//        }
//    }
    public void eval(String str) {
        for (String s : str.split(" ")) {
            collect(s);
        }
    }

    @Override
    public TypeInformation<String> getResultType() {
        return Types.STRING;
    }
}
