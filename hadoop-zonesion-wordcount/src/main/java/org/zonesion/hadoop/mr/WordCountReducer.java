package org.zonesion.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public  class WordCountReducer extends  Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
		int temp=0;
		for(IntWritable v:values){
			temp+=v.get();
		}
		context.write(key, new IntWritable(temp));
	}

}
