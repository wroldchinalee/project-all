package com.lwq.bigdata.flink.jsonparse;

import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/12/24
 * @description: FlatCol
 **/
public class FlatCol {
    private String name;
    private int index;
    private List<String> field;
    private String type;

    public FlatCol() {
    }

    public FlatCol(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public List<String> getField() {
        return field;
    }

    public void setField(List<String> field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
