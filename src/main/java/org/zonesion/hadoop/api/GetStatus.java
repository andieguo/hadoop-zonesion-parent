package org.zonesion.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class GetStatus {
	public static void getStatus(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		FileStatus fileStatus = fs.getFileStatus(path);
		System.out.println("ModificationTime is:" + fileStatus.getModificationTime());
		System.out.println("AccessTime is:"+fileStatus.getAccessTime());
		System.out.println("file Ower is:"+fileStatus.getOwner());
		System.out.println("file path is:"+fileStatus.getPath());
		
	}

	public static void main(String[] args) {
		// 读取文件最后修改时间
		try {
			getStatus("/user/hadoop/test/HelloWorld.mk");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
