package org.ncsu.mapreduce.common;

import java.util.logging.Level;

import org.ncsu.mapreduce.util.Logger;

/*
 * This class specifies all the output specifications for MapReduce framework
 */

public class MapReduceOutput {
	
	private String outputFileDirectory;
	private Class<?> reducerClass;
	
	public String getOutputFileDirectory() {
		return outputFileDirectory;
	}
	public void setOutputFileDirectory(String outputFileDirectory) {
		this.outputFileDirectory = outputFileDirectory;
	}
	public Class<?> getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(String reducerClass) {
		try {
			this.reducerClass = Class.forName(reducerClass);
		} catch (ClassNotFoundException e) {
			Logger.getLogger().log(Level.SEVERE,e.toString());
		}
	}

}
