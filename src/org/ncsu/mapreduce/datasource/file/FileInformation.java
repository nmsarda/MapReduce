package org.ncsu.mapreduce.datasource.file;

import java.io.File;

public class FileInformation {

	private String filePath;
	private String fileName;
	
	public FileInformation(String filePath, String fileName){
		this.fileName = fileName;
		this.filePath = filePath;
	}
	
	public String getFilePath(){
		return filePath;
	}
	
	public String getFileName(){
		return fileName;
	}
	
	public long getFileSize(){
		File f1 = new File(filePath);
		return f1.length();// to be written
	}
	
}
