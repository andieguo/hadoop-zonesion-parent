package org.zonesion.hadoop.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class ListFile {

	public static void list(String dst) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		Path dstPath = new Path(dst); // 目标路径
		FileStatus[] status = fs.listStatus(dstPath);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p);
		}
	}

	public static void main(String[] args) {
		try {
			ListFile.list("/user/hadoop/test");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
