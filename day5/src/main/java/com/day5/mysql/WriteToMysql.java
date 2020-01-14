package com.day5.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WriteToMysql {
	
	public static void main(String[] args) throws  Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(
				conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://localhost:3306/hadooptest",
				"root", 
				"root");
		Job job = Job.getInstance(conf, "readMysql");
		job.setJarByClass(WriteToMysql.class);
		//设置输入
		Path inputPaths = new Path("datas/out/mysql");
		FileInputFormat.setInputPaths(job, inputPaths);
		//设置map
		job.setMapperClass(WriteMap.class);
		job.setMapOutputKeyClass(DBReader.class);
		job.setMapOutputValueClass(NullWritable.class);
		//设置reduce
		job.setNumReduceTasks(0);
		//设置输出
		job.setOutputFormatClass(DBOutputFormat.class);
		String[] fields = {"word","number"};
		DBOutputFormat.setOutput(job, "wcresult", fields);
		//提交
		job.waitForCompletion(true);
	}
	
	public static class WriteMap extends Mapper<LongWritable, Text, DBReader, NullWritable>{
	
		private DBReader outputKey = new DBReader();
		private NullWritable outputValue = NullWritable.get();
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			this.outputKey.setWord(line.split("\t")[0]);
			this.outputKey.setNumber(Integer.valueOf(line.split("\t")[1]));
			context.write(outputKey, outputValue);
		}
	}


}
