package org.ncsu.mapreduce.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.ncsu.mapreduce.common.KeyValueClass;
import org.ncsu.mapreduce.common.MapReduceSpecification;

public class SortWrite {
	
	public void createAndSortLists(MapReduceSpecification spec,ArrayList<KeyValueClass<?, ?>> list,int mapperID,int threadID)
	{
		int noOfReducers = spec.getNoOfReducers();
		ArrayList<KeyValueClass<?, ?>>[] reduceLists = new ArrayList[noOfReducers];
		KeyValueClass<?,?> keyObj;
		HashPartitioner partition  = new HashPartitioner();
		int partitionNumber;
		
		for(int i = 0;i < noOfReducers;i++)
		{
			ArrayList<KeyValueClass<?, ?>> obj = new ArrayList<KeyValueClass<?, ?>>();
			reduceLists[i] = obj;
		}
		for(int i=0; i < list.size(); i++){
			keyObj = list.get(i);
			partitionNumber = partition.assignPartition(keyObj.getKey(), noOfReducers);
			reduceLists[partitionNumber].add(keyObj);
			
		}
		for(int i=0; i < list.size(); i++)
			list.remove(i);
		for(int i=0;i<noOfReducers;i++)
		{
			Collections.sort(reduceLists[i]);
			writeToFile(mapperID,threadID,i,reduceLists[i]);
		}
		
		
		
		System.out.println("hi");
		
		
		
		
		
	}
	
	private void writeToFile(Integer mapperID,Integer threadID,Integer reducerID,List list)
	{
		try {
			String fileName = "ReducerData"+File.separator+"Reducer_"+reducerID.toString()+File.separator+"Mapper"+mapperID.toString()+"_"+threadID.toString()+"_"+"Reducer"+reducerID.toString();
			FileWriter fwrite = new FileWriter(fileName);
			BufferedWriter bwrite = new BufferedWriter(fwrite);
			Iterator<KeyValueClass<String,Integer>> it = list.iterator();
			while(it.hasNext())
			{
				KeyValueClass<String, Integer> obj = it.next();
				bwrite.write(obj.getKey()+"   "+ obj.getValue());
				bwrite.newLine();
		
				
			}
			bwrite.close();
			fwrite.close();
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
