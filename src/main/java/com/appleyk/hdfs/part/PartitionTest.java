package com.appleyk.hdfs.part;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionTest extends Partitioner<Text, IntWritable> {

	/**
	 * key  		: map的输出key 
	 * value		: map的输出value 
	 * numReduceTask: reduce的task数量
	 * 返回值，指定reduce，从0开始
	 * 比如，分区0交由reducer0拿走
	 */
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTask) {
		
		if (key.toString().equals("a")) {
			//如果key的值等于a，则将其分区指定为0，对应第一个reducer拿走进行reduce
			return 0;
		} else {
			return 1;
		}
	}
}
