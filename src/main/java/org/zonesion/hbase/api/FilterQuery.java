package org.zonesion.hbase.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.zonesion.util.PropertiesHelper;

public class FilterQuery {
	/**
	 * SingleColumnValueFilter(byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value)
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", PropertiesHelper.getProperty("hbase.zookeeper.quorum"));
		HTable table = null;
		ResultScanner rs = null;
		try {
			table = new HTable(conf, "tb_admin");
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
					Bytes.toBytes("age"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("25")); // 当列info:age的值为25时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			rs = table.getScanner(s);
			for (Result result : rs) {
				System.out.format("ROW\t%s\n",new String(result.getRow()));
				for(KeyValue kv : result.raw()){
					System.out.format("COLUMN\t %S:%s\t%s\n",new String(kv.getFamily()),new String(kv.getQualifier()),new String(kv.getValue()));
				}
				System.out.println("----------------------");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(rs!=null) rs.close();
			if(table!=null)
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
}
