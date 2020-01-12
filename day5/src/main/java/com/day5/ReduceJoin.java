package com.day5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

/**
 *
 * 实现join
 *
 */
public class ReduceJoin extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //1 构造一个MapReduce job
        Job job = Job.getInstance(getConf(),"mrjob");
        job.setJarByClass(ReduceJoin.class);

        //2 配置job
        Path ordersPath = new Path("datas/mrjoin/orders.txt");
        Path productPath = new Path("datas/mrjoin/product.txt");
        TextInputFormat.setInputPaths(job, ordersPath,productPath);

        //map
        job.setMapperClass(MRJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle

        //reduce
        job.setReducerClass(MRJoinRedece.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        //output
        Path outputPath = new Path("datas/out/reduceJoin");
        TextOutputFormat.setOutputPath(job, outputPath);
        //如果输出目录已存在，就删除
        FileSystem hdfs = FileSystem.get(this.getConf());
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath,true);
        }

        //3 提交job
        return job.waitForCompletion(true) ? 0 : -1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new ReduceJoin(), args);
        System.exit(status);
    }

    public static class MRJoinMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit =(FileSplit)context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            if ("orders.txt".equals(fileName)){//订单数据
                String[] items = value.toString().split(",");
                outputKey.set(items[2]);//id
                outputValue.set(items[0]+"\t"+items[1]+"\t"+items[3]);
                context.write(outputKey,outputValue);
            }else {
                //商品数据
                String[] items = value.toString().split(",");
                outputKey.set(items[0]);
                outputValue.set(items[1]);
                context.write(outputKey,outputValue);
            }
        }
    }

    public static class MRJoinRedece extends Reducer<Text,Text,Text,Text> {
        private Text outputValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text value : values) {
                stringBuilder.append(value.toString()+"\t");
            }
            outputValue.set(stringBuilder.toString());
            context.write(key,outputValue);
        }
    }
}
