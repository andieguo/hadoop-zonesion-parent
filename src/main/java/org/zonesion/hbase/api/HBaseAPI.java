package org.zonesion.hbase.api;

import java.util.Arrays;

public class HBaseAPI {
	public static final String GET = 
			"	get <tableName> <rowKey>	\n" +
			"	get <tableName> <rowKey> <family>	\n" +
			"	get <tableName> <rowKey> <family> <column>	\n" ;
	public static final String PUT = 
			"	put <tableName> <rowKey> <family> <column> <value> \n" ;
	public static final String SCAN = "	scan <tableName> \n";
	public static final String CREATE = "	create <tableName> [family...]	\n";
	public static final String DELETE = "	delete <tableName>\n ";
	
	public static final String HELP = "HBaseAPI action ...\n" +CREATE+DELETE+PUT+SCAN+GET;
	
	public static void main(String[] args) {
		if(args.length == 0 || "help".equals(args[0])){
			System.out.println(HELP);
		}else{
			if("get".equals(args[0])){
				if(args.length == 3){
					GetRow.getRow(args[1],args[2]);
				}else if(args.length == 4){
					GetFamily.getFamily(args[1],args[2], args[3]);
				}else if(args.length == 5){
					GetColumn.getColumn(args[1],args[2],args[3],args[4]);
				}else{
					System.out.println(GET);
				}
			}
			if("scan".equals(args[0])){
				if(args.length == 2){
					GetScanner.list(args[1]);
				}else{
					System.out.println(SCAN);
				}
			}
			if("put".equals(args[0])){
				if(args.length == 6){
					System.out.println("Adding one record ...");
					PutRow.put(args[1],args[2],args[3],args[4],args[5]);
					System.out.format("Successfully added one record:%s\t%s\t%s\t%s\t%s\n",args[1],args[2],args[3],args[4],args[5]);
				}else{
					System.out.println(PUT);
				}
			}
			if("delete".equals(args[0])){
				if(args.length == 2){
					DeleteTable.deleteTable(args[1]);
					System.out.println("Succeefully deleted table "+args[1]);
				}else{
					System.out.println(DELETE);
				}
			}
			if("create".equals(args[0])){
				if(args.length > 2){
					String[] argss = Arrays.copyOfRange(args, 2, args.length);
					CreateTable.create(args[1], argss);
					System.out.println("Succeefully create table"+args[1]);
				}else{
					System.out.println(CREATE);
				}
			}
		}
	}
}
