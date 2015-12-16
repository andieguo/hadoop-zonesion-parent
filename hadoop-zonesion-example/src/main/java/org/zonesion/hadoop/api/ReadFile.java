package org.zonesion.hadoop.api;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.zonesion.util.PropertiesHelper;

public class ReadFile {
	// 读取文件的内容
	public static void read(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(filePath);
		FileStatus files[] = fs.listStatus(srcPath);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
		InputStream in = null;
		try {
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, System.out, 4096, false); // 复制到标准输出流
		} finally {
			IOUtils.closeStream(in);
		}
	}

	public static void main(String[] args) {
		try {
			ReadFile.read("/user/hadoop/test/HelloWorld.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
