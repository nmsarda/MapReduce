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
		int noOfFiles = dbConn.getNoOfFiles(spec.getMapReduceInput().getTableName());
		spec.getMapReduceInput().setNumberOfFiles(noOfFiles);
		ResultSet result = dbConn.executeSelect(spec.getMapReduceInput().getTableName());
		String fileName = "";	
		FileInformation fileInfo = null;
		try {			
			FileWriter fw = null;
			while(result.next()){
				String file = result.getString("FILENAME");
			//	if(!isFilePresent(fileName)){
				//if(!fileName.equalsIgnoreCase(file)){
				if(fw != null){						
						fw.close();
					}				
					fileName = file;
					File newFile = new File(fileName);
					if(!newFile.exists())
						{newFile.createNewFile(); //create a new file
							fileInfo = new FileInformation(newFile.getAbsoluteFile().toString(), fileName);
							spec.getMapReduceInput().setFiles(fileInfo); //set the input file information to the MapReduceSpecification object
						}
					fw = new FileWriter(newFile.getAbsoluteFile(), true);					
					String t1 = result.getString("CONTENT");
					t1.replaceAll("\n", System.getProperty("line.separator"));
					fw.write(t1);	
								
					
				}
											
			//}			
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
