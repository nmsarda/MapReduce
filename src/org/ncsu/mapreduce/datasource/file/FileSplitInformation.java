package org.ncsu.mapreduce.datasource.file;

import org.ncsu.mapreduce.common.MapReduceSpecification;

// Will contain split information for each file
public class FileSplitInformation {
	
	private FileInformation fileInfo;
	private long start;
	private long length;
	private MapReduceSpecification spec; 
	//This is required to get information like split size required,
	//number of mappers etc

	public FileSplitInformation(FileInformation fileInfo, long start, long length, MapReduceSpecification spec){
		this.fileInfo = fileInfo;
		this.start = start;
		this.length = length;
		this.spec = spec;
	}
	
	public FileInformation getFileInfo(){
		return fileInfo;
	}
	
	public long getStart(){
		return start;
	}
	
	public long getLength(){
		return length;
	}
	
	public MapReduceSpecification getSpec(){
		return spec;
	}
	
}
