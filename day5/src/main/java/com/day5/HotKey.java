package com.day5;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 实现join
 */
public class HotKey extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {

        //1 构造一个MapReduce job
        Job job = Job.getInstance(getConf(), "mrhotkey");
        job.setJarByClass(HotKey.class);

        //2 配置job
        Path ordersPath = new Path("datas/sougou_data/");
        TextInputFormat.setInputPaths(job, ordersPath);

        //map
        job.setMapperClass(WrodCoutMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
        job.setCombinerClass(WordCountRedece.class);

        /** 压缩
         *      Map input records=1724264
         * 		Map output records=1724264
         * 		Map output bytes=41454141
         * 		Map output materialized bytes=10971536 //map输出结果进行压缩
         * 		Input split bytes=765
         * 		Combine input records=1724264
         * 		Combine output records=411945
         * 		Reduce input groups=318824
         * 		Reduce shuffle bytes=10971536
         * 		Reduce input records=411945
         * 		Reduce output records=318824
         * 		Spilled Records=823890
         * 		Shuffled Maps =6
         * 		Failed Shuffles=0
         * 		Merged Map outputs=6
         */

        /** 不做压缩
         *		Map input records=1724264
         * 		Map output records=1724264
         * 		Map output bytes=41454141
         * 		Map output materialized bytes=44902753 //map输出结果
         * 		Input split bytes=765
         * 		Combine input records=0
         * 		Combine output records=0
         * 		Reduce input groups=318824
         * 		Reduce shuffle bytes=44902753
         * 		Reduce input records=1724264
         * 		Reduce output records=318824
         * 		Spilled Records=3448528
         * 		Shuffled Maps =6
         * 		Failed Shuffles=0
         * 		Merged Map outputs=6
         */

        //reduce
        job.setReducerClass(WordCountRedece.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(1);

        //output
        Path outputPath = new Path("datas/out/hotkey");
        TextOutputFormat.setOutputPath(job, outputPath);
        //如果输出目录已存在，就删除
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        //3 提交job
        return job.waitForCompletion(true) ? 0 : -1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new HotKey(), args);
        System.exit(status);
    }

    public static class WrodCoutMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            outputKey.set(words[2]);
            context.write(outputKey, outputValue);
        }
    }

    public static class WordCountRedece extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key,outputValue);
        }
    }
}
