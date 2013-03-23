package org.ncsu.mapreduce.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.ncsu.mapreduce.datasource.file.FileRecordReader;
import org.ncsu.mapreduce.datasource.file.FileSplitInformation;

public class MapRunner implements Runnable{
	//Runs the Mapper
	
	private int threadId;
	private FileSplitInformation split;
	MapReduceSpecification spec;
	public MapRunner(MapReduceSpecification spec, FileSplitInformation split, int id){
		this.split = split;
		this.threadId = id;
		this.spec = spec;
	}
	
	@SuppressWarnings("unchecked")
	public void run(){
		System.out.println("Mapper " + threadId + " Starting");
		FileRecordReader frr = new FileRecordReader(split);
		String temp1;		
		ArrayList<KeyValueClass<?, ?>> list = null ;
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
		for(int i=0; i < list.size(); i++){
			KeyValueClass<String, Integer> k = (KeyValueClass<String, Integer>) list.get(i);
			System.out.println(k.getKey() + " - " + k.getValue());
		}
	}
	
}
