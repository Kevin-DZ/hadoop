package com.day4.mobilestatistics.paritioner;

import com.day4.mobilestatistics.bean.FlowBean2;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区规则
 *
 */
public class FlowParitioner extends Partitioner<FlowBean2, NullWritable> {

    @Override
    public int getPartition(FlowBean2 key, NullWritable value, int numPartitions) {
        String phone = key.getPhone();
        if (phone.startsWith("159")){
            return 0;
        }else if (phone.startsWith("136") || phone.startsWith("138")){
            return 1;
        }else
            return 2;
    }
}
