package org.ncsu.mapreduce.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.logging.Level;

import org.ncsu.mapreduce.datasource.file.FileRecordReader;
import org.ncsu.mapreduce.datasource.file.FileSplitInformation;
import org.ncsu.mapreduce.util.Logger;
import org.ncsu.mapreduce.util.SortWrite;

/*
 * This class runs the map function given by the user.
 */

public class MapRunner implements Runnable{	
	
	private int threadID;
	private int mapperID;
	private FileSplitInformation split;
	private Mapper mapperInstance;
	MapReduceSpecification spec;
	public MapRunner(MapReduceSpecification spec, FileSplitInformation split, int id, int mapperId,Mapper mapperInstance){
		this.split = split;
		this.threadID = id;
		this.spec = spec;
		this.mapperID = mapperId;
		this.mapperInstance =  mapperInstance;
	}
	
	@SuppressWarnings("unchecked")
	public void run(){		
		/*
		 * Input given to the map function is in the form of line by line.
		 * Splits returned by the inputSplitter is fed to the record reader class.
		 * RecordReader returns line by line data.
		 */
		FileRecordReader frr = new FileRecordReader(split);
		String temp1;		
		ArrayList<KeyValueClass<?, ?>> list = null ;
		Logger.getLogger().log(Level.INFO,"Test");
		while((temp1=frr.getNext())!=null){
			try {
				/*
				 * Invoke Map function.
				 */
				//Method map = spec.getMapReduceInput().getMapperClass().getDeclaredMethod("map", String.class);
				//Object o1 = spec.getMapReduceInput().getMapperClass().newInstance();
				if(list == null)
					list = (ArrayList<KeyValueClass<?, ?>>) mapperInstance.map(temp1);
				else
					list.addAll((ArrayList<KeyValueClass<?, ?>>) mapperInstance.map(temp1));				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				Logger.getLogger().log(Level.SEVERE,e.toString());
			}			
		}
		frr.close();

		SortWrite sortWrite = new SortWrite();
		/*
		 * <Key, Value> returned by the map function is fed in a list.
		 * Once, the mapper completes processing of each split, 
		 * the list is sorted and hashed by sortWrite class
		 */
		sortWrite.createAndSortLists(spec, list,mapperID,threadID);
	}
	
}
