package com.lwq.java.jsonpath;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: LWQ
 * @create: 2020/12/3
 * @description: AmvchaJsonPathTest
 **/
public class AmvchaJsonPathTest {
    public static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        String json = getAmvcha();
        System.out.println(json);
        String db = (String) JsonPath.read(json, "$.__db");
        String table = (String) JsonPath.read(json, "$.__table");
        String curtGtid = (String) JsonPath.read(json, "$.__binlogCurtGTID");
        String type = (String) JsonPath.read(json, "$.__type");
        String binlogTime = (String) JsonPath.read(json, "$.__binlogTime");
        System.out.printf("db:%s, table:%s, curtGtid:%s, type:%s, binlogTime:%s\n",
                db, table, curtGtid, table, binlogTime);
        List<Map<String, String>> dataList = ((List<Map<String, String>>) JsonPath.read(json, "$.data"));
        for (int i = 0; i < dataList.size(); i++) {
            dataList.get(i).forEach((k, v) -> System.out.printf("key:%s,value:%s\n", k, v));
        }
        System.out.println("数据");
        Object read = JsonPath.read(json, "$.data");
        System.out.println(read);
        System.out.println("----------------分隔符-------------");
        Object read2 = JsonPath.read(json, "$.data[0]");
        System.out.println(read2);
        System.out.println("----------------分隔符-------------");
        Object read3 = JsonPath.read(json, "$.data[*]");
        System.out.println(read3);

    }

    public static String getAmvcha() {
        Amvcha amvcha = new Amvcha();
        amvcha.set__db("dcbsdb");
        amvcha.set__table("amvcha");
        amvcha.set__binlogCurtGTID("curtGtid001");
        amvcha.set__type("update");
        Map<String, String> map = new HashMap<String, String>();
        map.put("CRNT_ACCT_DATE", "20201123");
        map.put("TRAN_LOG_NUM", "SC040008890273");
        map.put("TRAN_INNR_SQNUM", "1");
        map.put("BANK_NUM", "001");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("CRNT_ACCT_DATE", "20201123");
        map2.put("TRAN_LOG_NUM", "SC040008890273");
        map2.put("TRAN_INNR_SQNUM", "1");
        map2.put("BANK_NUM", "001");
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        list.add(map);
        list.add(map2);
        amvcha.setData(list);

        String value = null;
        try {
            value = objectMapper.writeValueAsString(amvcha);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return value;
    }
}
