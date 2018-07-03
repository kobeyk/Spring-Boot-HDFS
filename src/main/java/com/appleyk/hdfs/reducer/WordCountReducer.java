package com.appleyk.hdfs.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 进行合并后数据的最终统计
 * 本次要使用的类型信息如下：
 * Text:Map输出的文本内容
 * IntWritable:Map处理的个数
 * Text:Reduce输出文本
 * IntWritable:Reduce的输出个数
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
   
	@Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context)
            		throws IOException, InterruptedException {
       
		//mapper的输出是reducer的输入，因此，这里打一下reducer的接收内容			
		List<Integer> list = new ArrayList<>();
		
		int sum = 0;//记录每个单词（key）出现的次数
        for (IntWritable value : values) {
        	//从values集合里面取出key的单个频率数量（其实就是1）进行叠加
        	int num = value.get();
            sum += num;
            list.add(num);
          
        }
        
    	/**
		 * mapper会把一堆key-value进行shuffle操作，其中涉及分区、排序以及合并（combine）
		 * 注：上述shuffle中的的合并（combine）区别于map最终的的合（归）并（merge）
		 * 比如有三个键值对：<a,1>,<b,1>,<a,1>
		 * combine的结果：<a,2>,<b,1>      == 被reducer取走，数据小
		 * merage 的结果；<a,<1,1>>,<b,1>  == 被reducer取走，数据较大（相比较上述combine来说）
		 * 注：默认combiner是需要用户自定义进行开启的，所以，最终mapper的输出其实是归并（merage）后的的结果
		 * 
		 * 所以，下面的打印其实就是想看一下mapper在shuffle这个过程后的merage结果（一堆key-values）
		 */
		System.out.println("key-values :<"+key+","+list.toString().replace("[", "<")
				.replace("]", ">")+">");
		
        //打印一下reduce的结果
        System.out.println("reduce计算结果 == key-value :<"+key+","+new IntWritable(sum)+">");
        //最后写入到输入输出上下文对象里面，传递给reducer进行shuffle，待所有reducer task完成后交由HDFS进行文件写入
        context.write(key, new IntWritable(sum));
        
     
    }
}