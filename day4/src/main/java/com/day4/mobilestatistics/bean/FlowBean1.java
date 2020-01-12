package com.day4.mobilestatistics.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean1 implements Writable {
    private long upPack;
    private long downPack;
    private long upFlow;
    private long downFlow;

    public FlowBean1() {
    }

    public void setAll(long upPack, long downPack, long upFlow, long downFlow) {
        setUpPack(upPack);
        setDownPack(downPack);
        setUpFlow(upFlow);
        setDownFlow(downFlow);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upPack);
        dataOutput.writeLong(this.downPack);
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upPack = in.readLong();
        this.downPack = in.readLong();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
    }

    public long getUpPack() {
        return upPack;
    }

    public void setUpPack(long upPack) {
        this.upPack = upPack;
    }

    public long getDownPack() {
        return downPack;
    }

    public void setDownPack(long downPack) {
        this.downPack = downPack;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downPack+"\t"+upFlow+"\t"+downFlow;
    }
}
