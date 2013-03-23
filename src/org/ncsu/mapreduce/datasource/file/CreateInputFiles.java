package org.ncsu.mapreduce.datasource.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.ncsu.mapreduce.common.MapReduceSpecification;
import org.ncsu.mapreduce.datasource.database.DBConnectionManager;

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
				if(!fileName.equalsIgnoreCase(file)){
					if(fw != null){						
						fw.close();
					}
					fileName = file;
					File newFile = new File(fileName);
					newFile.createNewFile();
					fw = new FileWriter(newFile.getAbsoluteFile());					
					FileInformation fileInfo = new FileInformation(newFile.getAbsoluteFile().toString(), fileName);
					spec.getMapReduceInput().setFiles(fileInfo);			
				}
				String t1 = result.getString("CONTENT");				
				//t1 = t1.replaceAll("\n", System.getProperty("line.separator"));	
				//System.setProperty("line.separator" ,"\n");
				//t1 = t1.replaceAll("\\r\\n|\\r|\\n", "\n");
				//t1 = t1.replaceAll("\\n", "\n");//System.getProperty("line.separator"));
				fw.write(t1);								
			}			
			fw.close();
		} catch (SQLException e) {			
			e.printStackTrace();
		} catch (IOException e) {			
			e.printStackTrace();
		}		
	}
	
}
