package com.day6.TopSogou;

import com.day6.javabean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SogouPartition extends Partitioner<SogouBean, NullWritable> {
    @Override
    public int getPartition(SogouBean key, NullWritable value, int numPartitions) {
        return (key.getUid().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
