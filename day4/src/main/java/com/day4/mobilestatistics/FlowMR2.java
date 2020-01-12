package com.day4.mobilestatistics;

import com.day4.mobilestatistics.bean.FlowBean2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 2
 * ------
 * 1 基于给定的数据统计实现每个手机号的上行包总和、下行包总和、上行流量总和、下行流量总和
 * 2 基于第一个需求的结果来进行处理，将结果数据按照上行包总和进行降序排序
 * 3 基于第一个程序的结果，将数据写入不同的文件
 *
 *
 */
public class FlowMR2 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //1 构造一个MapReduce job
        Job job = Job.getInstance(getConf(),"flow2");
        job.setJarByClass(FlowMR2.class);

        //2 配置job
        Path inputPath = new Path("datas/out/flow/flow1/part-r-00000");
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        job.setMapperClass(FlowMRMapper.class);
        job.setMapOutputKeyClass(FlowBean2.class);
        job.setMapOutputValueClass(NullWritable.class);

        //shuffle

        //reduce
        //job.setReducerClass(FlowMRRedece.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(FlowBean2.class);
        job.setNumReduceTasks(1);

        //output
        Path outputPath = new Path("datas/out/flow/flow2");
        //如果输出目录已存在，就删除
        FileSystem hdfs = FileSystem.get(this.getConf());
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath,true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        //3 提交job
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new FlowMR2(), args);
        System.exit(status);
    }

    public static class FlowMRMapper extends Mapper<LongWritable,Text,FlowBean2, NullWritable>{
        private FlowBean2 outputKey = new FlowBean2();
        private NullWritable outputValue = NullWritable.get();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().split("\t");
            if (array.length == 5){
                outputKey.setAll(
                        array[0],
                        Long.valueOf(array[1]),
                        Long.valueOf(array[2]),
                        Long.valueOf(array[3]),
                        Long.valueOf(array[4]));
                context.write(outputKey,outputValue);
            }else {
                return;
            }
        }
    }

    public static class FlowMRRedece extends Reducer<Text,FlowBean2,Text,FlowBean2>{

    }
}
