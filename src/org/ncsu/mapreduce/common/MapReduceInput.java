package org.ncsu.mapreduce.common;

import java.util.logging.Level;

import org.ncsu.mapreduce.datasource.database.DBConnectionParameters;
import org.ncsu.mapreduce.datasource.file.FileInformation;
import org.ncsu.mapreduce.util.Logger;

/*
 * This class specifies all the input specifications for MapReduce framework
 */


public class MapReduceInput {

	private String format;
	private Class<?> mapperClass;
	private String tableName;
	private FileInformation[] files; //change
	private DBConnectionParameters conn;
	private String inputDirectory=null;
	private int numberOfFiles;
	private int index;
	private Class<?> inputFormatClass;	
	
	public MapReduceInput(){
		index = 0;
		inputFormatClass = null;
	}
	
	/*
	 * Determines whether input is to be read from file or database
	 */
	public String getFormat(){
		return format;
	}
	/*
	 * user specifies the class which contains the map class
	 */
	public Class<?> getMapperClass(){
		return mapperClass;
	}
	/*
	 * Database table from which data is to be fetched.
	 */
	public String getTableName(){
		return tableName;
	}
	/*
	 * Input file information
	 */
	public FileInformation[] getFiles(){
		return files;
	}
	
	public DBConnectionParameters getDbConnectionParameters(){
		return conn;
	}
	
	public Class<?> getInputFormatClass() {
		return inputFormatClass;
	}
	
	public void setFormat(String format){
		this.format = format;
	}

	public void setMapperClass(String mapperClass){
		try {
			this.mapperClass = Class.forName(mapperClass);
		} catch (ClassNotFoundException e) {
			Logger.getLogger().log(Level.SEVERE,e.toString());
		}
	}
	
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	public void setFiles(FileInformation file){
		files[index++] = file;
	}
	
	public void setNumberOfFiles(int numberOfFiles){
		this.numberOfFiles = numberOfFiles;	
		files = new FileInformation[this.numberOfFiles];
	}	
	
	public void setDbConnectionParameters(DBConnectionParameters conn){
		this.conn = conn;
	}

	public void setInputFormatClass(String inputFormatClass){
		try {
			this.inputFormatClass = Class.forName(inputFormatClass);
		} catch (ClassNotFoundException e) {
			Logger.getLogger().log(Level.SEVERE,e.toString());
		}
	}

	public String getInputDirectory() {
		// TODO Auto-generated method stub
		return inputDirectory;
	}
	public void setInputDirectory(String inputDirectory) {
		// TODO Auto-generated method stub
		this.inputDirectory = inputDirectory;
	}
}
