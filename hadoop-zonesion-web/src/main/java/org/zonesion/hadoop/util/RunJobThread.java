package org.zonesion.hadoop.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.zonesion.hadoop.mr.WordCount;

public class RunJobThread implements Runnable {
	
	private String[] args;
	public RunJobThread(String[] args){
		this.args=args;
	}
	
	public void run() {
		try {
			Configuration conf=new Configuration();
			conf.set("fs.default.name", "hdfs://192.168.100.141:9000");
			conf.set("mapred.job.tracker", "http://192.168.100.141:9001");
			String path = RunJobThread.class.getClassLoader().getResource("").toString();
			path = path.substring(0, path.indexOf("classes"))+"lib/hadoop-zonesion-wordcount-1.0-SNAPSHOT.jar";
			System.out.println("wordcount:"+path);
			conf.set("mapred.jar", path);
			WordCount.runJob(conf,args);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//file:/E:/workspace/hadoop-zonesion-parent/hadoop-zonesion-web/target/classes/
		String path = RunJobThread.class.getClassLoader().getResource("").toString();
		path = path.substring(0, path.indexOf("classes"));
		System.out.println(path+"lib/hadoop-zonesion-wordcount-1.0-SNAPSHOT.jar");
		
	}

}
