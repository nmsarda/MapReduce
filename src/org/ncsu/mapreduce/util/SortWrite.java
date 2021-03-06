package org.ncsu.mapreduce.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import org.ncsu.mapreduce.common.KeyValueClass;
import org.ncsu.mapreduce.common.MapReduceSpecification;

/* Creates sorted sublists from the input received from the Mappers
 * and writes the sorted sublists to the file per Reducer.
 */
public class SortWrite {
	
	public void createAndSortLists(MapReduceSpecification spec,ArrayList<KeyValueClass<?, ?>> list,int mapperID,int threadID)
	{
		int noOfReducers = spec.getNoOfReducers();
		ArrayList<KeyValueClass<?, ?>>[] reduceLists = new ArrayList[noOfReducers];
		//ArrayList<ArrayList<KeyValueClass<?, ?>>> lists = new ArrayList<ArrayList<KeyValueClass<?, ?>>>();
		KeyValueClass<?,?> keyObj;
		Partitioner partition  = new HashPartitioner();
		int partitionNumber;
		
		for(int i = 0;i < noOfReducers;i++)
		{
			ArrayList<KeyValueClass<?, ?>> obj = new ArrayList<KeyValueClass<?, ?>>();
			reduceLists[i] = obj;
		}
		for(int i=0; i < list.size(); i++){
			keyObj = list.get(i);
			/* Get the partition number from the hash code of the key */
			partitionNumber = partition.assignPartition(keyObj.getKey(), noOfReducers);
			reduceLists[partitionNumber].add(keyObj);
			
		}
		for(int i=0; i < list.size(); i++)
		{
			/* Remove the objects from the main list */
			list.remove(i);
		}
		for(int i=0;i<noOfReducers;i++)
		{
			/* Sort the lists for the reducers */
			sortAscending(reduceLists[i]);
			writeToFile(mapperID,threadID,i,reduceLists[i]);
		}
	}
	public <T extends KeyValueClass<?, ?>> ArrayList<T> sortAscending(ArrayList<T> list){
	    Collections.sort(list);
	    return list;
	}
	
	private void writeToFile(Integer mapperID,Integer threadID,Integer reducerID,List<KeyValueClass<?, ?>> list)
	{
		try {
			String fileName = "ReducerData"+File.separator+"Reducer_"+reducerID.toString()+File.separator+"Mapper"+mapperID.toString()+"_"+threadID.toString()+"_"+"Reducer"+reducerID.toString();
			FileWriter fwrite = new FileWriter(fileName);
			BufferedWriter bwrite = new BufferedWriter(fwrite);
			Iterator<KeyValueClass<?,?>> it = list.iterator();
			while(it.hasNext())
			{
				KeyValueClass<?, ?> obj = it.next();
				/* Write to the reducer specific file */
				bwrite.write(obj.getKey()+" "+ obj.getValue());
				bwrite.newLine();
		
				
			}
			bwrite.close();
			fwrite.close();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		}
	}

}
