package org.zonesion.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CompanyJoinAddress {


	/*
	 * 在map中先区分输入行属于左表还是右表，然后对两列值进行分割， 保存连接列在key值，剩余列和左右表标志在value中，最后输出
	 * 输入：company01.txt
			 Beijing Red Star:1---<0,"Beijing Red Star:1">
			 Beijing JD:1--<0,"Beijing JD:1">
			 Shenzhen Tencent:2
			 address01.txt
			 1:Beijing--<0,"1:Beijing">
			 2:Shenzhen--<0,"2:Shenzhen">
	 * 输出：
	 		<"1","company:Beijing Red Star">
	 		<"1","company:Beijing JD">
	 		<"2","company:Shenzhen Tencent">
	 		<"1","address:Beijing">
	 		<"2","address:Shenzhen">
	 * 
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			Text addressId = new Text();
			Text info = new Text();
			String[] line = value.toString().split(":");// 获取文件的每一行数据，并以空格分割
			//获取输入文件的路径名
			String path = ((FileSplit) context.getInputSplit()).getPath().toString();
			if (line.length < 2) {
				return;
			}
			if (path.indexOf("company") >= 0) {//处理company文件的value信息： "Beijing Red Star:1"
				addressId.set(line[1]);//"1"
				info.set("company" + ":" + line[0]);//"company:Beijing Red Star"
				context.write(addressId,info);//<key,value> --<"1","company:Beijing Red Star">
			} else if (path.indexOf("address") >= 0) {//处理adress文件的value信息："1:Beijing"
				addressId.set(line[0]);//"1"
				info.set("address" + ":" + line[1]);//"address:Beijing"
				context.write(addressId,info);//<key,value> --<"1","address:Beijing">
			}
		}
	}
	/*
	 *  reduce解析map输出，将value中数据按照左右表分别保存，然后求出笛卡尔积，并输出。
	 *  1、输入：<key,value>---<"1",["company:Beijing Red Star","company:Beijing JD","address:Beijing"]>
	 * 2、Reduce对values的处理：
	 *    company=["Beijing Red Star","Beijing JD"],address=["Beijing"]
	 *  3、对company数组和address数组求笛卡尔积。
	 */
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			List<String> companys = new ArrayList<String>();
			List<String> addresses = new ArrayList<String>();
			Iterator<Text> it = values.iterator();//["company:Beijing Red Star","company:Beijing JD","address:Beijing"]
			while(it.hasNext()){
				String value = it.next().toString();//"company:Beijing Red Star"
				String[] result = value.split(":");
				if(result.length >= 2){
					if(result[0].equals("company")){
						companys.add(result[1]);
					}else if(result[0].equals("address")){
						addresses.add(result[1]);
					}
				}
			}
			// 求笛卡尔积
			if(0 != companys.size()&& 0 != addresses.size()){
				for(int i=0;i<companys.size();i++){
					for(int j=0;j<addresses.size();j++){
						context.write(new Text(companys.get(i)), new Text(addresses.get(j)));//<key,value>--<"Beijing JD","Beijing">
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: company Join address <companyTableDir> <addressTableDir> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "company Join address");
		//设置Job入口类
		job.setJarByClass(CompanyJoinAddress.class);
		// 设置Map和Reduce处理类
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//companyTableDir
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));//addressTableDir
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));//out
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
