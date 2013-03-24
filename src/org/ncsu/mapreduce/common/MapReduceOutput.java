package org.ncsu.mapreduce.common;

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
			System.out.println("Class with name " + reducerClass + " not found.");
		}
	}

}
