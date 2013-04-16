package org.ncsu.mapreduce.datasource.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;

import org.ncsu.mapreduce.common.MapReduceSpecification;
import org.ncsu.mapreduce.datasource.database.DBConnectionManager;
import org.ncsu.mapreduce.util.Logger;
/* Gets the data from the database and creates input files to be read by
 * the mappers.
 */
public class CreateInputFiles {

	private MapReduceSpecification spec;	
	
	public CreateInputFiles(MapReduceSpecification spec){
		this.spec = spec;
	}
	
	public void createFiles(){
		
		DBConnectionManager dbConn = new DBConnectionManager(spec.getMapReduceInput().getDbConnectionParameters());
		ResultSet result = dbConn.executeSelect(spec.getMapReduceInput().getTableName());
		String fileName = "";		
		try {			
			FileWriter fw = null;
			while(result.next()){
				String file = result.getString("FILENAME");
			//	if(!isFilePresent(fileName)){
				if(!fileName.equalsIgnoreCase(file)){
				if(fw != null){						
						fw.close();
					}
				System.out.println("\nhi");
					fileName = file;
					File newFile = new File(fileName);
					newFile.createNewFile(); //create a new file
					fw = new FileWriter(newFile.getAbsoluteFile());					
					FileInformation fileInfo = new FileInformation(newFile.getAbsoluteFile().toString(), fileName);
					spec.getMapReduceInput().setFiles(fileInfo); //set the input file information to the MapReduceSpecification object			
					
				}
				String t1 = result.getString("CONTENT");				
				fw.write(t1);								
			}			
			fw.close();
		} catch (SQLException e) {			
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		} catch (IOException e) {			
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		}		
	}
	/*private boolean isFilePresent(String fileName)
	{
		FileInformation fileInfo[] = spec.getMapReduceInput().getFiles();
		for(int i=0;i< fileInfo.length;i++)
		{
			if(fileName.equalsIgnoreCase(fileInfo[i].getFileName()))
			{
				return true;
			}
		}
		return false;
	
	}*/
	
}
