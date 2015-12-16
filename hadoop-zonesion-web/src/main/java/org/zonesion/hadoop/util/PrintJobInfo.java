package org.zonesion.hadoop.util;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;

import static org.zonesion.hadoop.util.Utils.*;

public class PrintJobInfo implements Runnable{
	
//	private JobConf jobConf;
	private JobStatus jobStatus;
	private static Configuration conf;
	static {
		conf=new Configuration();
		conf.set("mapred.job.tracker", Utils.JOBTRACKER);
	}
	
	public PrintJobInfo(Configuration conf) throws IOException{
	//	this.conf=conf;
		JobStatus[] jobStatusAll=new JobClient(new InetSocketAddress(HADOOP_HOST, HADOOP_PORT), conf).getAllJobs();

		this.jobStatus=jobStatusAll[jobStatusAll.length-1];
	}
	
	public PrintJobInfo(Configuration conf, int printTimes) throws IOException{
		this(conf);
	}

	public void run() {
		try {
			printJobStatus();
			printProgress();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void printJobStatus() throws IOException{
		
		loadJobStatus(conf);
		while(jobStatus.getRunState()!=0){
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try{			
				System.out.println("jobID:"+jobStatus.getJobID().getId()+",job run status:"+jobStatus.getRunState()+",job status,map:"+jobStatus.mapProgress()+",reduce:"+jobStatus.reduceProgress());
			}catch(Exception e){
				e.printStackTrace();
			}
		}
    }
	
	
	public void printProgress() throws IOException{
		JobStatus js=Utils.getJobStatus(conf);
		while(js.isJobComplete()){
			
			float[] progress=Utils.getMapReduceProgess(js);
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("map progress:"+progress[0]*100+"%,reduce progress:"+progress[1]*100+"%");
		}
	}
	
	public void loadJobStatus(Configuration conf) throws IOException{
		JobStatus[] jobStatusAll=new JobClient(new InetSocketAddress(HADOOP_HOST, HADOOP_PORT), conf).getAllJobs();
		this.jobStatus=jobStatusAll[jobStatusAll.length-1];
	}

}
