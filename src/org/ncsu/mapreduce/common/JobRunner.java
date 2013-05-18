package org.ncsu.mapreduce.common;


import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.ncsu.mapreduce.datasource.file.CreateInputFiles;
import org.ncsu.mapreduce.datasource.file.FileInformation;
import org.ncsu.mapreduce.datasource.file.FileSplitInformation;
import org.ncsu.mapreduce.datasource.file.FileSplitter;
import org.ncsu.mapreduce.util.Logger;

public class JobRunner {
/*
 * 
 * This is where the MapReduce framework kicks in. This is the master Node.
 * It allocates mappers and reducers their work. 
 * 	
 */
	@SuppressWarnings("unchecked")
	public void run(MapReduceSpecification spec){
		
		Logger.getLogger().log(Level.FINE,"In JobRunner!");
		Logger.getLogger().log(Level.INFO,"In JobRunner!");
		Logger.getLogger().log(Level.WARNING,"In JobRunner!");
		Logger.getLogger().log(Level.FINEST,"In JobRunner!");
		spec.calculateThreads();
		CreateInputFiles setUpFiles = new CreateInputFiles(spec);
		if(spec.getMapReduceInput().getInputDirectory() == null)
		{
			System.out.println("\nSetting up Input files from database. \n");
			setUpFiles.createFiles(); // Sets up the input files by reading from database.
		}
		else
		{
			setUpFiles.setFileInfo();
		}
		
		Class<?> temp;
		ArrayList<FileSplitInformation> splits = null;
		if((temp = spec.getMapReduceInput().getInputFormatClass()) != null){
			//User defined InputFormat Class			
			try {
				/*
				 * 
				 * Getting user specified getSplits class through Java Reflection
				 * 
				 */
				Method inputSplit = temp.getDeclaredMethod("getSplits", MapReduceSpecification.class);
				Object o1 = temp.newInstance();
				
				splits = (ArrayList<FileSplitInformation>) inputSplit.invoke(o1, spec);
				
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException  e) {				
				//e.printStackTrace();
				Logger.getLogger().log(Level.SEVERE,e.toString());
				
			}
		}
		else{	
			//if user has not specified any InputFormat Class, default splitter is used.
			
			/*
			 * Gets the different splits from getSplits and stores it in a arraylist
			 */
			System.out.println("\nCalculating input splits\n");
			splits = (new FileSplitter()).getSplits(spec); 
		}
		createReducerDirs(spec);
		
		/*
		 * 
		 * After the spliiting is done, now time for mappers to run.
		 * Thread Pool is created with number of threads equal to the number of mappers.
		 * Threads are then reused.
		 * Each Thread executes the map function
		 */
		System.out.println("\nMap Phase Starting");
		ExecutorService executor = Executors.newFixedThreadPool((int)spec.getNoOfMappers());			
		for(int i =0, k=0; i < splits.size(); i++, k = (k+1)%(int)spec.getNoOfMappers()){			
			executor.submit(new MapRunner(spec, splits.get(i), i, k)); // Creates new Thread for MapRunner
		}
		executor.shutdown(); //Stop accepting new tasks.
		try {
				/*
				 * waits for all threads to complete 
				 */
	           executor.awaitTermination(1, TimeUnit.DAYS); 
	    } catch (InterruptedException ex) {
	    	Logger.getLogger().log(Level.SEVERE,ex.toString());
	    	
	    }
	        System.out.println("\nMap Phase Completed");
	        System.out.println("\nReduce Phase Starting");
	    executor = Executors.newFixedThreadPool((int)spec.getNoOfReducers());
	    for(int i =0; i < (int)spec.getNoOfReducers(); i++){			
			executor.submit(new ReduceRunner(spec, i)); // Creates new Thread for ReducerRunner
		}
		executor.shutdown();
		try {
	           executor.awaitTermination(1, TimeUnit.DAYS); // waits for all threads to complete
	    } catch (InterruptedException ex) {            
	    }
		
		System.out.println("\nReduce Phase Completed");
		
		removeInputFiles(spec);
		removeReduceDirs(spec);
	}
	private void createReducerDirs(MapReduceSpecification spec)
	{
		File mainReduceDir = new File("ReducerData");
		mainReduceDir.mkdir();
		for(int i = 0;i<spec.getNoOfReducers();i++)
		{
			File subDir = new File("ReducerData"+File.separator+"Reducer_"+i);
			subDir.mkdir();
		}
				
	}
	private void removeReduceDirs(MapReduceSpecification spec)
	{	
		File mainDir = new File("ReducerData");
		for(int i = 0;i<spec.getNoOfReducers();i++)
		{
			File path = new File("ReducerData"+File.separator+"Reducer_"+i);			
			File[] files = path.listFiles();
			if(files != null)
			{
				for(File f: files) 
				{
					f.delete();
				}
			}
			path.delete();
		}
		mainDir.delete();
	}
	
	private void removeInputFiles(MapReduceSpecification spec)
	{
		FileInformation fileInfo[] = spec.getMapReduceInput().getFiles();
		for(int i=0;i<fileInfo.length;i++)
		{
			File file = new File(fileInfo[i].getFileName());
			file.delete();
		}
	}
		
}

