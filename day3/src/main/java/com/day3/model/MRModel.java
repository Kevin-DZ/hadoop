package com.day3.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MRModel extends Configured implements Tool {
    /**
     * 具体整个MapReduce job的定义：构建、配置、提交
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        /**
         * 构建一个job
         */
        //创建一个job的实例
        Job job = Job.getInstance(this.getConf(),"mrjob");
        //设置job运行的类
        job.setJarByClass(MRModel.class);

        /**
         * 配置job
         */
        //input：定义输入的方式，输入的路径
        Path inputPath = new Path(args[0]);
        TextInputFormat.setInputPaths(job,inputPath);
        //map：定义Map阶段的类及输出类型
        job.setMapperClass(MRModelMapper.class);
        job.setMapOutputKeyClass(null);
        job.setMapOutputValueClass(null);
        //shuffle：定义shuffle阶段实现的类
        //reduce：定义reduce阶段的类及输出类型
        job.setReducerClass(MRModelReduce.class);
        job.setOutputKeyClass(null);
        job.setOutputValueClass(null);
        job.setNumReduceTasks(1);//设置Reduce的个数，就是分区的个数
        //output：定义输出的类以及输出的路径
        Path outputPath = new Path(args[1]);
        //如果输出存在，就删除
        FileSystem hdfs = FileSystem.get(this.getConf());
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath,true);
        }
        TextOutputFormat.setOutputPath(job,outputPath);

        /**
         * 提交job：并根据job运行的结果返回
         */
        return job.waitForCompletion(true) ? 0:-1;
    }


    /**
     * 程序的入口
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //构建一个Conf对象，用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        //调用当前类的run方法
        int status = ToolRunner.run(conf, new MRModel(), args);
        //根据job运行的状态，来退出整个程序
        System.exit(status);
    }

    /**
     * 定义Mapper的实现类以及Map过程中的处理逻辑
     */
    public static class MRModelMapper extends Mapper<LongWritable, Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    /**
     * 定义Reducer的实现类以及Reduce过程中的处理逻辑
     */
    public static class MRModelReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
