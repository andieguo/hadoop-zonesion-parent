package org.zonesion.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ProductJoinPrice {
	/**
	 * 输入：product01.txt
	 * 		 	1 手表 
				
	 * 		 price01.txt
				1 $100
		输出：<key,value>--<1,"productname 手表">
							   <1,"money $100">
	 * 
	 * @author hadoop
	 *
	 */
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			Text productId = new Text();
			Text info = new Text();
			String[] line = value.toString().split(" ");// 获取文件的每一行数据，并以空格分割
			//获取输入文件的路径名
			String path = ((FileSplit) context.getInputSplit()).getPath().toString();
			if (line.length < 2) {
				return;
			}
			String productID = line[0];
			productId.set(productID);// key is product ID;
			if (path.indexOf("product") >= 0) {// 数据来自商品文件
				String productname = line[1];
				info.set("productname" + " " + productname);
			} else if (path.indexOf("price") >= 0) {// 数据来自支付文件
				String money = line[1];
				info.set("money" + " " + money);
			}
			context.write(productId,info);
		}
	}
	
	/**
	 * Mapper输出：<key,value>
	 * 							<1,"productname 手表">
							   <1,"money $100">
	 * 
	 * 
	 * Reduce输入：<key,value>
	 *							<1,["productname 手表","money $100"]> 
	 * 输出：
	 * 
	 * 
	 * @author hadoop
	 *
	 */
	public static class ReducerClass extends  Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			String productname = "";
			String money = "";
			Iterator<Text> it = values.iterator();//["productname 手表","money $100"]
			while(it.hasNext()){
				String value = it.next().toString();//"productname 手表"
				String[] result = value.split(" ");
				if(result.length >= 2){
					if(result[0].equals("productname")){
						productname = result[1];
					}else if(result[0].equals("money")){
						money = result[1];
					}
				}
			}
			//同时含有商品名和money名时，将两个数据写入output。
			if(!productname.equals("")&&!money.equals("")){
				context.write(new Text(productname), new Text(money));
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage:Join <productTableDir> <priceTableDir> <output>");
		}
		//定义Job
		Job job = new Job(conf,"join productTable and priceTable");
		//定义输入文件路径：商品表 & 价格表
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//<productTableDir>
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));//<priceTableDir>
		//定义输出文件路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));//<output>
		//设置Jar运行入口类和Mapper类和Reducer类
		job.setJarByClass(ProductJoinPrice.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		//设置输出文件的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : -1);
	}
}
