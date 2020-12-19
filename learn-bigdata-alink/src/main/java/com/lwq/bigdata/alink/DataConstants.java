package com.lwq.bigdata.alink;

import org.apache.flink.types.Row;

/**
 * @author: LWQ
 * @create: 2020/11/26
 * @description: DataConstants
 **/
public class DataConstants {

    //"row", "json", "vec", "kv", "csv", "f0", "f1"
    public static Row[] getData() {
        String str = "{\"f2\":\"4.0\",\"f4\":\"8.0\"}";
        Row[] rows = new Row[2];
        Row row1 = new Row(7);
        row1.setField(0, "1");
        row1.setField(1, "{\"f1\":\"1.0\",\"f2\":\"2.0\"}");
        row1.setField(2, "$3$1:1.0 2:2.0");
        row1.setField(3, "1:1.0,2:2.0");
        row1.setField(4, "1.0,2.0");
        row1.setField(5, "1.0");
        row1.setField(6, "2.0");
        rows[0] = row1;
        Row row2 = new Row(7);
        row2.setField(0, "2");
        row2.setField(1, "{\"f2\":\"4.0\",\"f4\":\"8.0\"}");
        row2.setField(2, "$3$1:4.0 2:8.0");
        row2.setField(3, "1:4.0,2:8.0");
        row2.setField(4, "4.0,8.0");
        row2.setField(5, "4.0");
        row2.setField(6, "8.0");
        rows[1] = row2;
        return rows;
    }

    public static String[] getCsvData() {
        Row[] rows = getData();
        String[] res = new String[rows.length];
        for (int i = 0; i < rows.length; i++) {
            res[i] = rows[i].getField(4).toString();
        }
        return res;
    }

    public static Row[] getColumsData() {
        Row[] rows = getData();
        Row[] ret = new Row[rows.length];
        for (int i = 0; i < rows.length; i++) {
            Row row = new Row(2);
            row.setField(0, rows[5]);
            row.setField(1, rows[6]);
            ret[i] = row;
        }
        return ret;
    }
}
