package com.lwq.bigdata.flink;

/**
 * Created by Administrator on 2020-11-26.
 */
public class Order {
    private long user;
    private String product;
    private int amount;

    public Order(long user, String product, int amount) {
        this.user = user;
        this.product = product;
        this.amount = amount;
    }

    public long getUser() {
        return user;
    }

    public void setUser(long user) {
        this.user = user;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }
}
