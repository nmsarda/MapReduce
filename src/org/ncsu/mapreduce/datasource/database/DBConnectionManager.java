package org.ncsu.mapreduce.datasource.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
/* The class which connects to the database.
 * 
 */

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
			e.printStackTrace();
		}
		catch (SQLException e) {			
			e.printStackTrace();
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
			e.printStackTrace();
		}
		return result;
	}
	
	public void close()
	{
		try {
			connection.close();//close the connection and the statement object
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}




}

