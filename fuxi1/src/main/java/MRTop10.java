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
import java.util.*;

/*
    统计搜狗 top10
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
        Job job = Job.getInstance(this.getConf(), "MRTop10");


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
        job.setMapOutputValueClass(IntWritable.class);
        //shuffle：定义shuffle阶段实现的类

        //reduce：定义reduce阶段的类及输出类型
        job.setReducerClass(MRModelReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);//设置Reduce的个数，就是分区的个数

        //output：定义输出的类以及输出的路径
        Path outputPath = new Path("datas/out/sougou/top10");
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
    public static class MRModelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private IntWritable outputValue =  new IntWritable(1);

        /**
         * 将每条数据的搜索词作为key，value恒为1
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            //将搜索词作为key
            outputKey.set(split[2]);
            context.write(outputKey,outputValue);
        }
    }

    /**
     * 定义Reducer的实现类以及Reduce过程中的处理逻辑
     */
    public static class MRModelReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        //在全局定义TreeMap，用于在reduce方法 中赋值，在cleanup方法中输出,默认按照key的升序排序
        TreeMap<Integer,String> top10 = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1; //表示降序
            }
        });

        private Text outputKey = new Text();
        private IntWritable outputValue =  new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //将当前搜索词的次数进行统计
            int sum  = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if (top10.size() < 10){
                //小于10,直接放
                if (top10.get(sum) != null){
                    //不为null.表示该个数已经存在
                    top10.put(sum,top10.get(sum) + "\t" + key.toString());
                }else {
                    //该个数不存在,直接放入
                    top10.put(sum,key.toString());
                }
            }else {//表示当前元素不小于10,插入以后,将最小的移除
                if (top10.get(sum) != null){
                    top10.put(sum,top10.get(sum) + "\t"+ key.toString());
                }else {
                    top10.put(sum,key.toString());
                    top10.remove(top10.lastKey());//删除最小的
                }
            }
        }

        /**
         * 用于将reduce赋值的TreeMap中的数据进行输出
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer,String> map:top10.entrySet()) {
                //获取前10名的次数，就是key
                Integer outValue = map.getKey();
                outputValue.set(outValue);
                //获取前10名的搜索词，就是map中的value,因为相同次数的搜索词拼接了 ，这时候分割以后输出
                String[] outputKeys = map.getValue().split("\t");
                for (String outKey : outputKeys) {
                    outputKey.set(outKey);
                    context.write(outputKey,outputValue);
                }
            }
        }
    }
}
