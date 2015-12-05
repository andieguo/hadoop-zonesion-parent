package org.zonesion.hbase;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.zonesion.util.PropertiesHelper;

public class WordCountHbaseReader {
	
	public static class WordCountHbaseReaderMapper extends 
		TableMapper<Text,Text>{

		@Override
		protected void map(ImmutableBytesWritable key,Result value,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer("");
			for(Entry<byte[],byte[]> entry:value.getFamilyMap("content".getBytes()).entrySet()){
				String str =  new String(entry.getValue());
				//将字节数组转换为String类型
				if(str != null){
					sb.append(new String(entry.getKey()));
					sb.append(":");
					sb.append(str);
				}
				context.write(new Text(key.get()), new Text(new String(sb)));
			}
		}
	}
	
	
	public static class WordCountHbaseReaderReduce extends Reducer<Text,Text,Text,Text>{
		private Text result = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text val:values){
				result.set(val);
				context.write(key, result);
			}
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		String tablename = "wordcount";
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", PropertiesHelper.getProperty("hbase.zookeeper.quorum"));
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 1) {
	      System.err.println("Usage: WordCountHbaseReader <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "WordCountHbaseReader");
	    job.setJarByClass(WordCountHbaseReader.class);
	    //设置任务数据的输出路径；
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
	    job.setReducerClass(WordCountHbaseReaderReduce.class);
	    Scan scan = new Scan();
	    TableMapReduceUtil.initTableMapperJob(tablename,scan,WordCountHbaseReaderMapper.class, Text.class, Text.class, job);
		//调用job.waitForCompletion(true) 执行任务，执行成功后退出；
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
