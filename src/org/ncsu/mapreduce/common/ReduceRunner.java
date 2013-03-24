package org.ncsu.mapreduce.common;

import java.util.ArrayList;

import org.ncsu.mapreduce.datasource.file.FileReaderAtReducer;

public class ReduceRunner implements Runnable {
	
	private int reducerId;
	private int threadId;
	public ReduceRunner(int reducerId, int threadId){
		this.reducerId = reducerId;
		this.threadId = threadId;
	}
	
	public void run(){
		FileReaderAtReducer fileReader = new FileReaderAtReducer(reducerId);
		String[] key;
		String currentKey = "", line;
		
		ArrayList<String> valueList = new ArrayList<String>();
		while((line = fileReader.getNext())!= null){
			System.out.println("Reducer "+reducerId + " - " + line);
			key = line.split(" ");
			if(currentKey.equals("")){
				currentKey = key[0];
				valueList.add(key[1]);
				continue;
			}
			if(currentKey.equalsIgnoreCase(key[0])){
				valueList.add(key[1]);
			}
			else{
				currentKey = key[0];
				//Reduce function
			}
		}
	}
	
	

}
