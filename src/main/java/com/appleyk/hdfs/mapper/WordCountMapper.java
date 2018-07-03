package com.appleyk.hdfs.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper 原型 ： Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * 
 * KEYIN	: 默认情况下，是mr框架所读到的一行文本内容的起始偏移量，Long,
 * 		                  但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long,而用LongWritable
 * VALUEIN  : 默认情况下，是mr框架所读到的一行文本的内容(Java String 对应 Hadoop中的Text)
 * KEYOUT   : 用户自定义逻辑处理完成之后输出数据中的key，在此处是单词(String)，同上用Text
 * VALUEOUT : 用户自定义逻辑处理完成之后输出数据中的value，在这里是单词的次数：Integer，对应Hadoop中的IntWritable
 * 
 * mapper的输入输出参数的类型必须和reducer的一致，且mapper的输出是reducer的输入
 * 
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年7月3日15:41:13
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	 /**
	 * map实现数据拆分的操作
     * 本操作主要进行Map的数据处理
     * 在Mapper父类里面接受的内容如下：
     * LongWritable：文本内容的起始偏移量
     * Text:每行的文本数据（行内容）
     * Text:每个单词分解后的统计结果
     * IntWritable：输出记录的结果
     */
	 @Override
     protected void map(LongWritable key, Text value,Context context)
             throws IOException, InterruptedException {
       
		 System.out.println("文本内容的起始偏移量："+key);
         String line = value.toString()    ;//取出每行的数据
         String[] result  = line.split(" ");//按照空格进行数据拆分
         //循环单词
         for (int i = 0 ;i <result.length ; i++){
            
        	 //针对每一个单词，构造一个key-value
        	 System.out.println("key-value : <"+new Text(result[i])+","+new IntWritable(1)+">");
        	 
        	
        	 /**
        	  * 将每个单词的key-value写入到输入输出上下文对象中
        	  * 并传递给mapper进行shuffle过程，待所有mapper task完成后交由reducer进行对号取走
        	  */
             context.write(new Text(result[i]), new IntWritable(1));
         }
         
         /**			   map端的shuffle过程（大致简单的描述一下）
          *                       |
          *                       |  放缓存（默认100M，溢出比是0.8，即80M满进行磁盘写入并清空，
          *                       |  剩余20M继续写入缓存，二者结合完美实现边写缓存边溢写（写磁盘））
          *                       V
          *               <b,1>,<c,1>,<a,1>,<a,1>
          *               		  
          *                       |
          *                       | 缓存写满了，开始shuffle（洗牌、重组）  == 包括分区，排序，以及可进行自定的合并（combine）
          *                       V     
          * 写入磁盘文件（not hdfs）并进行文件归并，成一个个的大文件 <a,<1,1>>,<b,1>,<c,1>   
          * 
          *               		  |
          *               		  |
          *               		  V
          *   每一个大文件都有各自的分区，有几个分区就对应几个reducer，随后文件被各自的reducer领走
          *   
          *           !!! 这就是所谓的mapper的输入即是reducer的输出 !!!
          */
     }
}
