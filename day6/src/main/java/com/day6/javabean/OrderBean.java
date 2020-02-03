package com.day6.javabean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String pId;
    private double price;

    public OrderBean() {
    }

    public void setAll(String orderId, String pId, double price) {
        setOrderId(orderId);
        setpId(pId);
        setPrice(price);
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + pId + "\t" + price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int comp = getOrderId().compareTo(o.getOrderId());
        //如果订单id相同，按照价格排序
        if (comp == 0) {
            return -Double.valueOf(getPrice()).compareTo(Double.valueOf(o.getPrice()));
        }
        return comp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        orderId = input.readUTF();
        pId = input.readUTF();
        price = input.readDouble();
    }
}
