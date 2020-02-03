package com.day6.partiton;

import com.day6.javabean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
