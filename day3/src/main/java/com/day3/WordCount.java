package com.day3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCount extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //1 构造一个MapReduce job
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCount.class);

        //2 配置job
        Path inputPath = new Path("datas/word");
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle

        //reduce
        job.setReducerClass(WordCountRedece.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //output
        Path outputPath = new Path("datas/out/wordcount");
        TextOutputFormat.setOutputPath(job, outputPath);

        //3 提交job
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WordCount(), args);
        System.exit(status);
    }

    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text outputKey =  new Text();
        private IntWritable outputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                outputKey.set(word);
                context.write(outputKey,outputValue);
            }
        }
    }

    public static class WordCountRedece extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key,outputValue);
        }
    }
}
