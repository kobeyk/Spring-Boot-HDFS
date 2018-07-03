package com.appleyk.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.appleyk.hdfs.mapper.WordCountMapper;
import com.appleyk.hdfs.part.PartitionTest;
import com.appleyk.hdfs.reducer.WordCountReducer;

/**
 * MapReduce任务的Client端，主要用来提交Job
 * @author yukun24@126.com
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年7月3日-上午9:51:49
 */
public class WordCountApp {

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		//配置uri
		conf.set("fs.defaultFS", "hdfs://192.168.142.138:9000");
	
		//创建一个作业，作用在Hadoop集群上（remote）
		Job job = Job.getInstance(conf, "wordCount");
		
		/**
		 * 设置jar包的主类（如果样例demo打成Jar包扔在Linux下跑任务，
		 * 	需要指定jar包的Main Class,也就是指定jar包运行的主入口main函数）
		 */
		job.setJarByClass(WordCountApp.class);
		
		//设置Mapper 任务的类（自己写demo实现map）
		job.setMapperClass(WordCountMapper.class);
		//设置Reducer任务的类（自己写demo实现reduce）
		job.setReducerClass(WordCountReducer.class);

		//指定mapper的分区类
		//job.setPartitionerClass(PartitionTest.class);
		
		//设置reducer（reduce task）的数量（从0开始）
		//job.setNumReduceTasks(2);
		
		
		//设置映射输出数据的键（key）      类（型）
		job.setMapOutputKeyClass(Text.class);
		//设置映射输出数据的值（value）类（型）
		job.setMapOutputValueClass(IntWritable.class);

		//设置作业（Job）输出数据的键（key）      类（型）    == 最后要写入到输出文件里面
		job.setOutputKeyClass(Text.class);
		//设置作业（Job）输出数据的值（value）类（型）   == 最后要写入到输出文件里面
		job.setOutputValueClass(IntWritable.class);

		//设置输入的Path列表（可以是单个文件也可以是多个文件（目录表示即可））
		FileInputFormat.setInputPaths (job, new Path("hdfs://192.168.142.138:9000/input" ));
		//设置输出的目录Path（确认输出Path不存在，如存在，请先进行目录删除）
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.142.138:9000/output"));

		//将作业提交到集群并等待它完成。
		boolean bb =job.waitForCompletion(true);
		
		if (!bb) {
			System.out.println("Job任务失败！");
		} else {
			System.out.println("Job任务成功！");
		}
	}

}
