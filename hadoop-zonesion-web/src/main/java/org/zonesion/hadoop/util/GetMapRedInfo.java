package org.zonesion.hadoop.util;

import static org.zonesion.hadoop.util.Utils.HADOOP_HOST;
import static org.zonesion.hadoop.util.Utils.HADOOP_PORT;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;

public class GetMapRedInfo {
	public String[] getMapRed(String jobName) throws IOException{
		JobStatus[] jobStatusAll=new JobClient(new InetSocketAddress(HADOOP_HOST, HADOOP_PORT), Utils.conf).getAllJobs();
		
		JobStatus jobStatus=jobStatusAll[jobStatusAll.length-1];
		boolean flag=true;
		while(flag){
			if(flag){
				
			}
		}
		String[] mapred=new String[2];
		return mapred;
	}
}
