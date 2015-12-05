package org.zonesion.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class CheckFile {

	 public static void check(String filePath) throws IOException{
    	 	Configuration conf = new Configuration();
    	 	conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
         FileSystem fs = FileSystem.get(conf);
         Path path = new Path(filePath);
         boolean isExists = fs.exists(path);
         System.out.println("isexist:"+isExists);
    }
	
	public static void main(String[] args) {
			try {
				check("/user/hadoop/test/HelloWorld.mk");
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
}
