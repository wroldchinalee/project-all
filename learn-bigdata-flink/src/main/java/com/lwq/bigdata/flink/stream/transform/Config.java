package com.lwq.bigdata.flink.stream.transform;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: Config
 **/
public class Config {
    private String sql;

    public Config(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
