package com.lwq.bigdata.flink.jsonparse;

import org.apache.flink.types.Row;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author: LWQ
 * @create: 2021/1/5
 * @description: NRow
 **/
public class NRow implements Serializable {
    private String[] fieldNames;
    private Row row;

    public NRow() {
    }

    public NRow(Row row, String[] fieldNames) {
        checkNotNull(fieldNames, "FieldNames should not be null.");
        checkArgument(row.getArity()==fieldNames.length,
                "Number of field values and names is different.");
        this.row = row;
        this.fieldNames = fieldNames;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }
}
