package org.zonesion.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class MkdirFolder {
	// 创建目录
	public static void mkdir(String path) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("create dir ok!");
		} else {
			System.out.println("create dir failure");
		}
		fs.close();
	}

	public static void main(String[] args) {
		// 创建文件夹
		try {
			MkdirFolder.mkdir("/user/hadoop/test");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
