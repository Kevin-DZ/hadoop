package com.day4;

import com.day4.bean.WordCountBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 使用自定义数据类型
 *
 * 将wordcount中map的结果进行输出，包含三列：单词、单词长度、1
 *
 *
 */
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
        job.setMapOutputKeyClass(WordCountBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle

        //reduce
        //job.setReducerClass(WordCountRedece.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);

        //output
        Path outputPath = new Path("datas/out/wordcount");
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
        int status = ToolRunner.run(conf, new WordCount(), args);
        System.exit(status);
    }

    public static class WordCountMapper extends Mapper<LongWritable,Text,WordCountBean,IntWritable>{
        private WordCountBean outputKey =  new WordCountBean();
        private IntWritable outputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                outputKey.setCountBean(word,word.length());
                context.write(outputKey,outputValue);
            }
        }
    }

}
