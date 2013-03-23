package org.ncsu.mapreduce.common;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.ncsu.mapreduce.datasource.file.CreateInputFiles;
import org.ncsu.mapreduce.datasource.file.FileSplitInformation;
import org.ncsu.mapreduce.datasource.file.FileSplitter;

public class JobRunner {
	
	@SuppressWarnings("unchecked")
	public void run(MapReduceSpecification spec){
		System.out.println("In JobRunner!");
		CreateInputFiles setUpFiles = new CreateInputFiles(spec);
		setUpFiles.createFiles(); // Sets up the input files by reading from database.
		Class<?> temp;
		ArrayList<FileSplitInformation> splits = null;
		if((temp = spec.getMapReduceInput().getInputFormatClass()) != null){
			//User defined InputFormat Class			
			try {
				Method inputSplit = temp.getDeclaredMethod("getSplits", MapReduceSpecification.class);
				Object o1 = temp.newInstance();
				splits = (ArrayList<FileSplitInformation>) inputSplit.invoke(o1, spec);
				
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException  e) {				
				e.printStackTrace();
			}
		}
		else{
			
			splits = (new FileSplitter()).getSplits(spec);
		}
		ExecutorService executor = Executors.newFixedThreadPool((int)spec.getNoOfMappers());			
		for(int i =0, k=0; i < splits.size(); i++, k = (k+1)%(int)spec.getNoOfMappers()){			
			executor.submit(new MapRunner(spec, splits.get(i), i, k)); // Creates new Thread for MapRunner
		}
		executor.shutdown();
		try {
	           executor.awaitTermination(1, TimeUnit.DAYS); // waits for all threads to complete
	    } catch (InterruptedException ex) {            
	    }
	        System.out.println("Completed");
	}
		
}

