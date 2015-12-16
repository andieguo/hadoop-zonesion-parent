package org.zonesion.hadoop.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.zonesion.util.PropertiesHelper;

public class CreateFile {
	//创建新文件
    public static void create(String dst , byte[] contents) throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.default.name", PropertiesHelper.getProperty("fs.default.name"));
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }
    
    public static void create(String dst ,String contents) throws IOException{
    	create(dst, contents.getBytes());
    }
    
    public static void main(String[] args) {
    	//测试创建文件
	    byte[] contents =  "hello world 世界你好\n".getBytes();
	    try {
			CreateFile.create("/user/hadoop/test/HelloWorld.txt",contents);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
}
