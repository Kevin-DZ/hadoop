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
import java.util.HashSet;
import java.util.Set;

/*
    统计搜狗 uv
 */

public class MRTop10 extends Configured implements Tool {
    /**
     * 具体整个MapReduce job的定义：构建、配置、提交
     *
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
        Job job = Job.getInstance(this.getConf(), "mruv");


        //设置job运行的类
        job.setJarByClass(MRTop10.class);

        /**
         * 配置job
         */
        //input：定义输入的方式，输入的路径
        Path inputPath = new Path("datas/sougou_data/");
        TextInputFormat.setInputPaths(job, inputPath);

        //map：定义Map阶段的类及输出类型
        job.setMapperClass(MRModelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //shuffle：定义shuffle阶段实现的类

        //reduce：定义reduce阶段的类及输出类型
        job.setReducerClass(MRModelReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);//设置Reduce的个数，就是分区的个数

        //output：定义输出的类以及输出的路径
        Path outputPath = new Path("datas/out/sougou/uv");
        //如果输出存在，就删除
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        /**
         * 提交job：并根据job运行的结果返回
         */
        return job.waitForCompletion(true) ? 0 : -1;
    }


    /**
     * 程序的入口
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //构建一个Conf对象，用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        //调用当前类的run方法
        int status = ToolRunner.run(conf, new MRTop10(), args);
        //根据job运行的状态，来退出整个程序
        System.exit(status);
    }

    /**
     * 定义Mapper的实现类以及Map过程中的处理逻辑
     */
    public static class MRModelMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String hours = split[0].substring(0, 2);
            outputKey.set(hours);
            outputValue.set(split[1]);
            context.write(outputKey,outputValue);
        }
    }

    /**
     * 定义Reducer的实现类以及Reduce过程中的处理逻辑
     */
    public static class MRModelReduce extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable outputValue = new IntWritable();

        Set<String> sets = new HashSet<>();
        /**
         *
         * @param key 每个小时
         * @param values 这个小时的所有用户
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            sets.clear();
            //将这个小时的所有用户id放入set集合自动去重
            for (Text value : values) {
                sets.add(value.toString());
            }
            outputValue.set(sets.size());

            context.write(key, outputValue);
        }
    }
}
