package org.ncsu.mapreduce.datasource.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
/* The class which connects to the database.
 * 
 */
import java.util.logging.Level;

import org.ncsu.mapreduce.util.Logger;

public class DBConnectionManager {
	
	private  Connection connection=null;
	private Statement statement = null;

	public DBConnectionManager(DBConnectionParameters conn){
		try
		{
			Class.forName(conn.getDbDriver());
			connection = DriverManager.getConnection(conn.getJdbcURL(), conn.getUsername(), conn.getPassword());//create the connection			
			statement = connection.createStatement();// creates the connection statement
			System.out.println("In DbConnectionManager, connection setup!");
		}
		catch (ClassNotFoundException e)
		{			
			Logger.getLogger().log(Level.SEVERE,e.toString());
		}
		catch (SQLException e) {			
			Logger.getLogger().log(Level.SEVERE,e.toString());
		}
	}
	/* Retrieve the contents from a table specified by the user
	 * 
	 */
	public ResultSet executeSelect(String tablename)
	{
		ResultSet result=null;
		try
		{
			result = statement.executeQuery("SELECT * FROM "+tablename);//Retrieves data from the supplied table name
		}
		catch (Exception e)
		{			
			Logger.getLogger().log(Level.SEVERE,e.toString());
		}
		return result;
	}
	
	public int getNoOfFiles(String tablename)
	{
		ResultSet result=null;
		int files=0;
		try
		{
			result = statement.executeQuery("SELECT count(unique filename) AS count FROM "+tablename);			
			while(result.next())
			{				
				files = result.getInt("count");
			}
			System.out.println("\nNumber of Files in the input : "+ files);
			
		}
		catch(Exception e)
		{
			Logger.getLogger().log(Level.SEVERE,e.toString());
			e.printStackTrace();
			
		}
		try {
			result.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return files;
	}
	
	public void close()
	{
		try {
			connection.close();//close the connection and the statement object
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		}
	}




}

