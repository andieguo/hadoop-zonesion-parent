package org.zonesion.hbase.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.zonesion.util.PropertiesHelper;

public class DeleteTable {

	public static void main(String[] args) {
		deleteTable("tb_admin");
	}

	public static void deleteTable(String tableName) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", PropertiesHelper.getProperty("hbase.zookeeper.quorum"));
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {// 表存在
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}else{
				System.out.println(tableName + "不存在！");
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(admin!=null){
				try {
					System.out.println("关闭HBaseAdmin");
					admin.close();
					System.exit(0);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
