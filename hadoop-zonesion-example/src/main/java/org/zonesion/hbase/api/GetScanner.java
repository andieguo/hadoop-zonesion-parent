package org.zonesion.hbase.api;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.zonesion.util.PropertiesHelper;

public class GetScanner {

	public static void main(String[] args) {
		list("tb_admin");
	}

	public static void list(String tableName) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", PropertiesHelper.getProperty("hbase.zookeeper.quorum"));
		HTable table = null;
		ResultScanner rs = null;
		try {
			Scan scan = new Scan(); 
			table = new HTable(conf, tableName);
			rs = table.getScanner(scan);
			for(Result row : rs){
				System.out.format("ROW\t%s\n",new String(row.getRow()));
				for(Map.Entry<byte[], byte[]> entry : row.getFamilyMap("info".getBytes()).entrySet()){
					String column = new String(entry.getKey());
					String value = new String(entry.getValue());
					System.out.format("COLUMN\t info:%s\t%s\n",column,value);
				}
				for(Map.Entry<byte[], byte[]> entry : row.getFamilyMap("address".getBytes()).entrySet()){
					String column = new String(entry.getKey());
					String value = new String(entry.getValue());
					System.out.format("COLUMN\t address:%s\t%s\n",column,value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(rs!=null)rs.close();
			if(table!=null)
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
}
