package com.lwq.java.jsonpath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/12/3
 * @description: Amvcha
 **/
public class Amvcha {
    private String __db;
    private String __table;
    private String __binlogCurtGTID;
    private String __binlogTime;
    private String __type;
    private List<Map<String, String>> data = new ArrayList<Map<String, String>>();

    public Amvcha() {
    }

    public String get__db() {
        return __db;
    }

    public void set__db(String __db) {
        this.__db = __db;
    }

    public String get__table() {
        return __table;
    }

    public void set__table(String __table) {
        this.__table = __table;
    }

    public String get__binlogCurtGTID() {
        return __binlogCurtGTID;
    }

    public void set__binlogCurtGTID(String __binlogCurtGTID) {
        this.__binlogCurtGTID = __binlogCurtGTID;
    }

    public String get__binlogTime() {
        return __binlogTime;
    }

    public void set__binlogTime(String __binlogTime) {
        this.__binlogTime = __binlogTime;
    }

    public String get__type() {
        return __type;
    }

    public void set__type(String __type) {
        this.__type = __type;
    }

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }
}
