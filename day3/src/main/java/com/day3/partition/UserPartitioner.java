package com.day3.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPartitioner extends Partitioner<Text,IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String s = key.toString();
        if ("浦东".equals(s) || "长宁".equals(s)){
            return 1;
        }else
            return 0;

    }
}
