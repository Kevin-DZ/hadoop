package com.day6.TopSogou;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SogouBean implements WritableComparable<SogouBean> {

    private String uid;
    private String values;

    public SogouBean() {
    }

    public void setAll(String uid, String values) {
        setUid(uid);
        setValues(values);
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }

    @Override
    public int compareTo(SogouBean o) {
        int comp = getUid().compareTo(o.getUid());
        //如果订单id相同，按照价格排序
        if (comp == 0) {
            //return -Integer.valueOf(getValues()).compareTo(Integer.valueOf(o.getValues()));
        }
        return comp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(values);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        uid = input.readUTF();
        values = input.readUTF();
    }

    @Override
    public String toString() {
        return uid + "\t" + values;
    }
}
