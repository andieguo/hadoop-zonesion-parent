package org.zonesion.hadoop;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class EarthQuakesPerDate {

	public static class EarthQuakesPerDateMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (key.get() > 0) {
				try {
					String[] lines = value.toString().split(",");
					SimpleDateFormat format = new SimpleDateFormat(
							"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
					SimpleDateFormat format2 = new SimpleDateFormat(
							"yyyy-MM-dd");
					String str = format2.format(format.parse(lines[0]));
					context.write(new Text(str), new IntWritable(1));// //输出<key,value>为<word,one>-----<2012-01-02,1>

				} catch (ParseException e) {
					System.out.println(e.toString());
				}

			}

		}

	}
	
	public static class EarthQuakesPerDatePatitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			String day = key.toString().split("-")[2];
			if(Integer.valueOf(day)%2 == 0){
				return 0;
			}
			return 1;
		}
	}
	
	public static class EarthQuakesPerDateCombiner extends
		Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}

	}

	public static class EarthQuakesPerDateReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}

	}
	
	// 在MapReduce中，由Job对象负责管理和运行一个计算任务，并通过Job的一些方法对任>务的参数进行相关的设置。
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: EarthQuakesPerDate <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "EarthQuakesPerDate");
		job.setJarByClass(EarthQuakesPerDateMapper.class);
		// 使用EarthQuakesPerDateMapper类完成Map过程；
		job.setMapperClass(EarthQuakesPerDateMapper.class);
		// 使用EarthQuakesPerDateCombiner类完成Combiner过程；
		job.setCombinerClass(EarthQuakesPerDateCombiner.class);
		// 使用EarthQuakesPerDatePatitioner类完成Partitioner过程；
		job.setPartitionerClass(EarthQuakesPerDatePatitioner.class);
		// 使用EarthQuakesPerDateReducer类完成Reducer过程；
		job.setReducerClass(EarthQuakesPerDateReducer.class);
		// 设置Reduce的个数为2；
		job.setNumReduceTasks(2);
		// 设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；
		job.setOutputKeyClass(Text.class);
		// 设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；
		job.setOutputValueClass(IntWritable.class);
		// 设置任务数据的输入路径；
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// 设置任务输出数据的保存路径；
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// 调用job.waitForCompletion(true) 执行任务，执行成功后退出；
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
