package org.ncsu.mapreduce.common;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.ncsu.mapreduce.datasource.database.DBConnectionParameters;
import org.ncsu.mapreduce.datasource.file.FileInformation;
class node{
	int a;
	public void tp2(){System.out.println("Hi");}
}

//import org.ncsu.mapreduce.common.MapReduceInput.node;
public class MapReduceInput {

	private String format;
	private Class<?> mapperClass;
	private String tableName;
	private FileInformation[] files; //change
	private DBConnectionParameters conn;
	private int numberOfFiles;
	private int index;
	private Class<?> inputFormatClass;	
	
	public MapReduceInput(){
		index = 0;
		inputFormatClass = null;
	}
	
	public String getFormat(){
		return format;
	}
	
	public Class<?> getMapperClass(){
		return mapperClass;
	}
	
	public String getTableName(){
		return tableName;
	}
	
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
			System.out.println("Class with name " + mapperClass + " not found.");
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
	
	public void temp(Class<?> a){
		
			Method a1;
			try {
				a1 = a.getDeclaredMethod("tp2");
				try {
					//a1.setAccessible(true);
					Object o1 = a.newInstance();
					a1.invoke(o1);
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	}
	
	public void setDbConnectionParameters(DBConnectionParameters conn){
		this.conn = conn;
	}

	public void setInputFormatClass(String inputFormatClass){
		try {
			this.inputFormatClass = Class.forName(inputFormatClass);
		} catch (ClassNotFoundException e) {
			System.out.println("Class with name " + inputFormatClass + " not found.");
		}
	}
}
