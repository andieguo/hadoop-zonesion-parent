package org.zonesion.hbase.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.zonesion.util.PropertiesHelper;

public class GetFamily {

	public static void main(String[] args){
		getFamily("tb_admin","andieguo","info");
	}
	
	public static void getFamily(String tableName,String rowKey,String family) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", PropertiesHelper.getProperty("hbase.zookeeper.quorum"));
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Get query = new Get(rowKey.getBytes());
			query.addFamily(Bytes.toBytes(family));
			if(table.exists(query)){
				Result result = table.get(query);
				System.out.format("ROW\t%s\n",new String(result.getRow()));
				for(KeyValue kv : result.raw()){
					System.out.format("COLUMN\t %S:%s\t%s\n",new String(kv.getFamily()),new String(kv.getQualifier()),new String(kv.getValue()));
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
