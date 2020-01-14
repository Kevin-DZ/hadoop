package com.day5.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 CREATE table wcresult(
 id int PRIMARY KEY auto_increment,
 word VARCHAR(20),
 number int
 );

 INSERT INTO wcresult(word,number) VALUES('adc',10);
 INSERT INTO wcresult(word,number) VALUES('efc',11);
 INSERT INTO wcresult(word,number) VALUES('dad',22);
 INSERT INTO wcresult(word,number) VALUES('gds',33);
 INSERT INTO wcresult(word,number) VALUES('dsac',44);
 INSERT INTO wcresult(word,number) VALUES('csadsa',55);
 INSERT INTO wcresult(word,number) VALUES('ggh',66);
 INSERT INTO wcresult(word,number) VALUES('jk',77);
 INSERT INTO wcresult(word,number) VALUES('ert',88);
 INSERT INTO wcresult(word,number) VALUES('zt',99);
 */

public class ReadFromMysql {
	
	public static void main(String[] args) throws  Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(
				conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://localhost:3306/hadooptest",
				"root", 
				"root");
		Job job = Job.getInstance(conf, "readFromMysql");
		job.setJarByClass(ReadFromMysql.class);
		//设置输入:
		job.setInputFormatClass(DBInputFormat.class);
		String[] fields = {"word","number"};
		DBInputFormat.setInput(
				job, 
				DBReader.class, 
				"wcresult",
				null, 
				"number",
				fields
				);
		//设置map
		job.setMapperClass(ReadMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		//设置reduce
		job.setNumReduceTasks(0);
		//设置输出
		Path outputPath = new Path("datas/out/mysql");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		//提交
		job.waitForCompletion(true);
	}
	
	public static class ReadMap extends Mapper<LongWritable, DBReader, Text, IntWritable>{
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void map(LongWritable key, DBReader value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			this.outputKey.set(value.getWord());
			this.outputValue.set(value.getNumber());
			context.write(outputKey,outputValue);
		}
	}


}
