package org.zonesion.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class DeleteFile {
	// 删除文件
	public static void delete(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		boolean isok = fs.deleteOnExit(path);
		if (isok) {
			System.out.println("delete ok!");
		} else {
			System.out.println("delete failure");
		}
		fs.close();
	}

	public static void main(String[] args) {
		// 测试删除文件
		try {
			DeleteFile.delete("/user/hadoop/test/HelloWorld.mk");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
