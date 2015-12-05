package org.zonesion.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MapSideJoin {
	/**
	 * 数据输入：
	 * 　customers-01.txt　　　　　　　　　　　　　　　orders-01.txt
　　　　 1,Stephanie Leung,555-555-5555　　　　　   1,book,12.95,02-Jun-2008
　　　　　　　　　　　　 									  1,car,88.25,20-May-2008
		Map处理输出：
	 * @author hadoop
	 *
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		private Map<String, String> joinData = new HashMap<String,String>();
		
		@Override
		protected void setup(Context context) throws InterruptedException, IOException {
			//获取设置在DistributedCache中的文件路径【在job中设置将customers表放入DistributedCache中】
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(cacheFiles != null && cacheFiles.length > 0){
				String line;
				String[] tokens;
				BufferedReader joinReader = null;
				try {
					for (Path path : cacheFiles) {
						joinReader = new BufferedReader(new FileReader(path.toString()));
						while ((line = joinReader.readLine()) != null) {// 读取DistributedCache中文件数据
							tokens = line.split(",",2);//1,Stephanie Leung,555-555-5555
							if(tokens.length >= 2){
								joinData.put(tokens[0], tokens[1]);// joinData["1","Stephanie Leung,555-555-5555"]
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					joinReader.close();
				}
			}
		}
		// <key,value>--<10,"1,book,12.95,02-Jun-2008">　　　　　 
		// <key,value>--<20,"1,car,88.25,20-May-2008">
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//将value 与 joinData中相应记录进行join
			String[] values = value.toString().split(",",2);//value = "1,book,12.95,02-Jun-2008"; values[0] = "1"; values[1] = "book,12.95,02-Jun-2008";
			String joinValue = joinData.get(values[0]);//joinData["1","Stephanie Leung,555-555-5555"]
			if(joinValue != null){
				context.write(new Text(values[0]), new Text(joinValue+","+values[1]));//<"1","Stephanie Leung,555-555-5555,book,12.95,02-Jun-2008">
			}
		}
	}
	
	/**
	 * 输入：
	 * <"1",["Stephanie Leung,555-555-5555,book,12.95,02-Jun-2008","Stephanie Leung,555-555-5555,car,88.25,20-May-2008"]>
	 * 输出：
	 * @author hadoop
	 *
	 */
	public static class ReducerClass extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text value : values){
				context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage: map side join <customers> <orders> <out>");
		}
		Job job = new Job(conf,"map side join");
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReducerClass.class);
		//默认
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输出文件的key和value类型(一定要配置)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//将第一个数据源（假定是较小的那个）放置到DistributedCache
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
		//设置路径
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//调用job.waitForCompletion(true) 执行任务，执行成功后退出；
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
