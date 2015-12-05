package org.zonesion.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class UploadFile {
	// 上传本地文件
	public static void upload(String src, String dst) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(src); // 原路径
		Path dstPath = new Path(dst); // 目标路径
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		fs.copyFromLocalFile(false, srcPath, dstPath);
		// 打印文件路径
		System.out.println("------------list files------------" + "\n");
		FileStatus[] fileStatus = fs.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fs.close();
	}

	public static void main(String[] args) {
		// 测试上传文件
		try {
			UploadFile.upload(
					"/usr/hadoop/workspace/MapReduce/HDFSAPI/src/config.properties",
					"/user/hadoop/test/config.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
