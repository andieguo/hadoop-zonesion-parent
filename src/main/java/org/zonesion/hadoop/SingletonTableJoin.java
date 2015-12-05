package org.zonesion.hadoop;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SingletonTableJoin{

	//Map将输入分割成child和parent，然后正序输出一次作为右表，反序输出一次作为
	//左表，需要注意的是在输出的value中必须加上左右表区别标志
	public static class JoinMapper extends Mapper<Object, Text, Text, Text>{
		/**
		 * 输入child parent
				Tom Lucy
				Lucy Mary
				Lucy Ben

			输出左表(反序输出，并添加左表标志)：
				<key,value>--<"Lucy","1 Tom">
				<key,value>--<"Mary","1 Lucy">
				<key,value>--<"Ben","1 Lucy">
			输出右表（正序输出，并添加右表标志）：
				<key,value>--<"Tom","2 Lucy">
				<key,value>--<"Lucy","2 Mary">
				<key,value>--<"Lucy","2 Ben">
		 */
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String childName = new String();
			String parentName = new String();
			String relationType = new String();
			String line = value.toString();
			String[] values = line.split(" ");
			if(values.length >= 2){
				if(values[0].compareTo("child") != 0){//忽略child parent行
					childName = values[0];
					parentName = values[1];
					relationType = "1";
					context.write(new Text(parentName), new Text(relationType+" "+childName));//<"Lucy","1 Tom">
					relationType = "2";
					context.write(new Text(childName), new Text(relationType+" "+parentName));//<"Jone","2 Lucy">
				}
			}
		}
	}
	/**
	 * Map的输出：
	 * 		输出左表(反序输出，并添加左表标志)：
				<key,value>--<"Lucy","1 Tom">
				<key,value>--<"Mary","1 Lucy">
				<key,value>--<"Ben","1 Lucy">
			输出右表（正序输出，并添加右表标志）：
				<key,value>--<"Tom","2 Lucy">
				<key,value>--<"Lucy","2 Mary">
				<key,value>--<"Lucy","2 Ben">
		Reduce的输入：
			输入：<key,value>--<"Lucy",["1 Tom","2 Mary","2 Ben"]>
									<"Mary",["1 Lucy"]>
									<"Ben",[1 Lucy]>
									<"Tom",["2 Lucy"]>
			处理values数据：
				  grandChild=[Tom],grandParent = [Mary Ben]----写入HDFS
				  grandChild=[Lucy],grandParent = []--Lcuy没有爷爷
				  grandChild=[Lucy],grandParent = []--Lcuy没有爷爷
				  grandChild=[],grandParent = [Lucy]--Lcuy没有孙子
	 * 
	 * 
	 * @author hadoop
	 *
	 */
	public static class JoinReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			List<String> grandChild = new ArrayList<String>();//孙子
			List<String> grandParent = new ArrayList<String>();//爷爷
			Iterator<Text> it = values.iterator();//["1 Tom","2 Mary","2 Ben"]
			while(it.hasNext()){
				String[] record = it.next().toString().split(" ");//"1 Tom"---[1,Tom]
				if(record.length == 0) continue;
				if(record[0].equals("1")){//左表，取出child放入grandchild
					grandChild.add(record[1]);
				}else if(record[0].equals("2")){//右表，取出parent放入grandParent
					grandParent.add(record[1]);
				}
			}
			//grandchild 和 grandparent数组求笛卡尔积
			if(grandChild.size() != 0 && grandParent.size() != 0){
				for(int i=0;i<grandChild.size();i++){
					for(int j=0;j<grandParent.size();j++){
						context.write(new Text(grandChild.get(i)), new Text(grandParent.get(j)));
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: SingletonTableJoin <in> <out>");
		}
		Job job = new Job(conf,"SingletonTableJoin Job");
		job.setJarByClass(SingletonTableJoin.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : -1);
	}
	
}
