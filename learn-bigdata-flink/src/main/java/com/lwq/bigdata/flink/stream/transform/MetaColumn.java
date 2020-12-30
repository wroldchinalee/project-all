package com.lwq.bigdata.flink.stream.transform;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: MetaColumn
 **/
public class MetaColumn {
    private String name;
    private String type;

    public MetaColumn(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
