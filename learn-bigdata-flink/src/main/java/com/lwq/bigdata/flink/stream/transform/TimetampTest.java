package com.lwq.bigdata.flink.stream.transform;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: TimetampTest
 **/
public class TimetampTest {
    public static void main(String[] args) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp);

        Date date1 = new Date(System.currentTimeMillis());
        System.out.println(date1);

        Time time = new Time(System.currentTimeMillis());
        System.out.println(time);
    }
}
