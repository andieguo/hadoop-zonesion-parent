package org.zonesion.hadoop.api;

import java.io.IOException;

public class HDFSAPI {
	public static final String MKDIR = "	mkdir <foldername>	\n";
	public static final String UPLOAD = "	put <localfile> <remotefile> \n" ;
	public static final String LIST = "	list <foldername> \n";
	public static final String CREATEFILE = "	create <remotefile> <content> \n";
	public static final String CAT = "	cat <remotefile>\n ";
	public static final String RENAME = "	rename <oldfile> <newfile>\n ";
	public static final String STATUS = "	status <remotefile>\n";
	public static final String DELETE = "	delete <tableName>\n ";
	public static final String ISEXIST = "	isexist <remotefile>\n";
	public static final String HELP = "HDFSAPI action ...\n" 
				+MKDIR+UPLOAD+LIST+CREATEFILE+CAT+RENAME+STATUS+DELETE+ISEXIST;
	
	
	public static void main(String[] args) throws IOException {
		if(args.length == 0 || "help".equals(args[0])){
			System.out.println(HELP);
		}else{
			if("mkdir".equals(args[0])){
				if(args.length == 2){
					MkdirFolder.mkdir(args[1]);
				}else{
					System.out.println(MKDIR);
				}
			}
			if("put".equals(args[0])){
				if(args.length == 3){
					UploadFile.upload(args[1], args[2]);
				}else{
					System.out.println(UPLOAD);
				}
			}
			if("list".equals(args[0])){
				if(args.length == 2){
					ListFile.list(args[1]);
				}else{
					System.out.println(LIST);
				}
			}
			if("create".equals(args[0])){
				if(args.length == 3){
					CreateFile.create(args[1], args[2]);
				}else{
					System.out.println(CREATEFILE);
				}
			}
			if("cat".equals(args[0])){
				if(args.length == 2){
					ReadFile.read(args[1]);
				}else{
					System.out.println(CAT);
				}
			}
			if("rename".equals(args[0])){
				if(args.length == 3){
					RenameFile.rename(args[1], args[2]);
				}else{
					System.out.println(RENAME);
				}
			}
			if("status".equals(args[0])){
				if(args.length == 2){
					GetStatus.getStatus(args[1]);
				}else{
					System.out.println(STATUS);
				}
			}
			if("delete".equals(args[0])){
				if(args.length == 2){
					DeleteFile.delete(args[1]);
				}else{
					System.out.println(DELETE);
				}
			}
			if("isexist".equals(args[0])){
				if(args.length == 2){
					CheckFile.check(args[1]);
				}else{
					System.out.println(ISEXIST);
				}
			}
		}
	}
}
