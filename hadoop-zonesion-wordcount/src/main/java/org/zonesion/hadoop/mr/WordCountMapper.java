package org.zonesion.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public  class WordCountMapper extends  Mapper<LongWritable,Text,Text,IntWritable> {
	

	
	@Override
	public void map(LongWritable key,Text value, Context context)throws IOException , InterruptedException{
		String[] text=value.toString().split(" ");
		for(int i=0;i<text.length;i++){
			context.write(new Text(text[i]), new IntWritable(1));
		}
	}


}