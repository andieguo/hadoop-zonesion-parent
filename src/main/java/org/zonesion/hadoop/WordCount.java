package org.zonesion.hadoop;

/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  //TokenizerMapper继承Mapper类，并重写其map方法。
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
   	//map方法中的value值存储的是文本文件中的一行信息（以回车符为行结束标记）。
	//map方法中的key值为该行的首字符相对与文本文件的首地址的偏移量。
	//StringTokenizer类将每一行拆分成一个个的单词，并将<word,1>作为map方法的结果输出。
	//其中IntWritable和Text类是Hadoop对int和String类的序列化封装，这些类能够被序列化，以便在分布式环境中进行数据交换。
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);//输出<key,value>为<word,one>
      }
    }
  }
  //IntSumReducer继承Reducer类，并重写其reduce方法。
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
	//reduce方法的输入参数key为单个单词;
	//而Iterable<IntWritable> values为各个Mapper上对应单词的计数值所组成的列表。
      int sum = 0;
      for (IntWritable val : values) {//遍历求和
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);//输出求和后的<key,value>
    }
  }

  //在MapReduce中，由Job对象负责管理和运行一个计算任务，并通过Job的一些方法对任务的参数进行相关的设置。
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
	//使用TokenizerMapper类完成Map过程；
    job.setMapperClass(TokenizerMapper.class);
	//使用IntSumReducer类完成Combiner过程；
    job.setCombinerClass(IntSumReducer.class);
	//使用IntSumReducer类完成Reducer过程；
    job.setReducerClass(IntSumReducer.class);
	//设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
    job.setOutputKeyClass(Text.class);
	//设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；
    job.setOutputValueClass(IntWritable.class);
	//设置任务数据的输入路径；
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	//设置任务输出数据的保存路径；
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	//调用job.waitForCompletion(true) 执行任务，执行成功后退出；
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
