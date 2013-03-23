package org.ncsu.mapreduce.datasource.database;

public class DBConnectionParameters {
	
	private String jdbcURL;
	private String dbDriver;
	private String username;
	private String password;
	
	public void setJdbcURL(String jdbcURL){
		this.jdbcURL = jdbcURL;
	}
	
	public void setDbDriver(String dbDriver){
		this.dbDriver = dbDriver;
	}
	
	public void setUserName(String username){
		this.username = username;
	}
	
	public void setPassword(String password){
		this.password = password;
	}
	
	public String getJdbcURL(){
		return jdbcURL;
	}
	
	public String getDbDriver(){
		return dbDriver;
	}
	
	public String getUsername(){
		return username;
	}
	
	public String getPassword(){
		return password;
	}
}
