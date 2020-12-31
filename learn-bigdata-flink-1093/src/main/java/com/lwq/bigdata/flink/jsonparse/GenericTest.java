package com.lwq.bigdata.flink.jsonparse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * Created by Administrator on 2020-12-30.
 */
public class GenericTest {
    public static void main(String[] args) {
        TypeInformation<Row> type = TypeInformation.of(Row.class);
        System.out.println(type.getTypeClass());
    }
}
