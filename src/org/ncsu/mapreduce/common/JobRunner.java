package org.ncsu.mapreduce.common;


import java.io.File;
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
/*
 * 
 * This is where the MapReduce framework kicks in. This is the master Node.
 * It allocates mappers and reducers their work. 
 * 	
 */
	@SuppressWarnings("unchecked")
	public void run(MapReduceSpecification spec){
		System.out.println("In JobRunner!");
		spec.calculateThreads();
		CreateInputFiles setUpFiles = new CreateInputFiles(spec);
		setUpFiles.createFiles(); // Sets up the input files by reading from database.
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
				e.printStackTrace();
			}
		}
		else{	
			//if user has not specified any InputFormat Class, default splitter is used.
			
			/*
			 * Gets the different splits from getSplits and stores it in a arraylist
			 */
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
	    }
	        System.out.println("Completed");
	    
	    executor = Executors.newFixedThreadPool((int)spec.getNoOfReducers());
	    for(int i =0; i < (int)spec.getNoOfReducers(); i++){			
			executor.submit(new ReduceRunner(spec, i)); // Creates new Thread for ReducerRunner
		}
		executor.shutdown();
		try {
	           executor.awaitTermination(1, TimeUnit.DAYS); // waits for all threads to complete
	    } catch (InterruptedException ex) {            
	    }
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
			System.out.println(path);
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
		
}

