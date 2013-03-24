package org.ncsu.mapreduce.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.ncsu.mapreduce.datasource.file.FileReaderAtReducer;

public class ReduceRunner implements Runnable {
	
	private int reducerId;
	private BufferedWriter fileWriter;
	private MapReduceSpecification spec;
	public ReduceRunner(MapReduceSpecification spec, int reducerId){
		this.spec = spec;
		this.reducerId = reducerId;				
		String completeFilePath = spec.getMapReduceOutputClass().getOutputFileDirectory() + System.getProperty("file.separator") + "Reducer" + reducerId;
		File subDir = new File(spec.getMapReduceOutputClass().getOutputFileDirectory());
		subDir.mkdir();
		try {
			this.fileWriter = new BufferedWriter(new FileWriter(completeFilePath));
		} catch (IOException e) {
			e.printStackTrace();
		}			
	}
	
	public void run(){
		FileReaderAtReducer fileReader = new FileReaderAtReducer(reducerId);
		String[] key;
		String currentKey = "", line;
		
		ArrayList<String> valueList = null;
		while((line = fileReader.getNext())!= null){
			System.out.println("Reducer "+reducerId + " - " + line);
			key = line.split(" ");
			if(currentKey.equals("")){
				valueList = new ArrayList<String>();
				currentKey = key[0];
				valueList.add(key[1]);
				continue;
			}
			if(currentKey.equalsIgnoreCase(key[0])){
				valueList.add(key[1]);
			}
			else{				
				try {
					Method reduce = spec.getMapReduceOutputClass().getReducerClass().getDeclaredMethod("reduce", String.class, ArrayList.class);
					Object o1 = spec.getMapReduceOutputClass().getReducerClass().newInstance();					
					String value = (String) reduce.invoke(o1, currentKey, valueList);
					writeInFile(currentKey, value);
				} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {					
					e.printStackTrace();
				}
				currentKey = key[0];
				valueList = new ArrayList<String>();
				valueList.add(key[1]);
			}
		}
		if(valueList!=null){ //For the boundary case
			try {
				Method reduce = spec.getMapReduceOutputClass().getReducerClass().getDeclaredMethod("reduce", String.class, ArrayList.class);
				Object o1 = spec.getMapReduceOutputClass().getReducerClass().newInstance();					
				String value = (String) reduce.invoke(o1, currentKey, valueList);
				writeInFile(currentKey, value);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {					
				e.printStackTrace();
			}
		}
		try {
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeInFile(String key, String value){
		try {
			fileWriter.write(key + " " + value);
			fileWriter.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
