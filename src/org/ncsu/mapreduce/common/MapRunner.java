package org.ncsu.mapreduce.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.ncsu.mapreduce.datasource.file.FileRecordReader;
import org.ncsu.mapreduce.datasource.file.FileSplitInformation;
import org.ncsu.mapreduce.util.SortWrite;

public class MapRunner implements Runnable{
	//Runs the Mapper
	
	private int threadID;
	private int mapperID;
	private FileSplitInformation split;
	MapReduceSpecification spec;
	public MapRunner(MapReduceSpecification spec, FileSplitInformation split, int id, int mapperId){
		this.split = split;
		this.threadID = id;
		this.spec = spec;
		this.mapperID = mapperId;
	}
	
	@SuppressWarnings("unchecked")
	public void run(){
		
		FileRecordReader frr = new FileRecordReader(split);
		String temp1;		
		ArrayList<KeyValueClass<?, ?>> list = null ;
		System.out.println("ThreadID "+Thread.currentThread().getId());
		while((temp1=frr.getNext())!=null){
			try {
				Method map = spec.getMapReduceInput().getMapperClass().getDeclaredMethod("map", String.class);
				Object o1 = spec.getMapReduceInput().getMapperClass().newInstance();
				if(list == null)
					list = (ArrayList<KeyValueClass<?, ?>>) map.invoke(o1, temp1);
				else
					list.addAll((ArrayList<KeyValueClass<?, ?>>) map.invoke(o1, temp1));				
				
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		frr.close();
		
		/*for(int i=0; i < list.size(); i++){
			KeyValueClass<String, Integer> k = (KeyValueClass<String, Integer>) list.get(i);		

		}*/
		SortWrite sortWrite = new SortWrite();
		sortWrite.createAndSortLists(spec, list,mapperID,threadID);
	}
	
}
