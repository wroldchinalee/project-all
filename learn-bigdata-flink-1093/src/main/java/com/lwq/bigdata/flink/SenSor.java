package com.lwq.bigdata.flink;

/**
 * Created by Administrator on 2020-11-26.
 */
public class SenSor {
    private String id;
    private long timestamp;
    private double temp;


    public SenSor(String id, long timestamp, double temp) {
        this.id = id;
        this.timestamp = timestamp;
        this.temp = temp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SenSor{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", temp=" + temp +
                '}';
    }
}
