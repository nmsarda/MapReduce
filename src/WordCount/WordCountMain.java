package WordCount;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.ncsu.mapreduce.common.*;
import org.ncsu.mapreduce.datasource.database.DBConnectionParameters;
public class WordCountMain {
	
	public static void main(String[] args) {
		
		MapReduceSpecification spec = new  MapReduceSpecification();
		MapReduceInput inp = spec.add_input();
		MapReduceOutput op = spec.output();
		DBConnectionParameters dbConn = new DBConnectionParameters();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(args[0]));			
			int count = -1;
			String line;
			while((line=br.readLine())!=null){
				count++;
				line=line.split("=")[1];
				if(line.equals("")){
					continue;
				}
				switch(count){
				case 0:{					
					spec.setNoOfThreads(Integer.parseInt(line));
					continue;
				}
				case 1:{
					spec.setNoOfMappers(Integer.parseInt(line));
					continue;
				}
				case 2:{
					spec.setNoOfReducers(Integer.parseInt(line));
					continue;
				}
				case 3:{
					spec.minByteSize(Integer.parseInt(line));
					continue;
				}
				case 4:{
					
					dbConn.setDbDriver(line);
					continue;					
				}
				case 5:{
					dbConn.setJdbcURL(line);
					continue;
				}
				case 6:{					
					dbConn.setUserName(line);
					continue;					
				}
				case 7:{
					dbConn.setPassword(line);
					inp.setDbConnectionParameters(dbConn);
					continue;
				}
				case 8:{							
					inp.setTableName(line);
					continue;										
				}
				case 9:{
					inp.setFormat(line);
					continue;
				}
				case 10:{					
					inp.setMapperClass(line);
					continue;
				}
				case 11:{
					//inp.setNumberOfFiles(Integer.parseInt(line));
					continue;
				}
				case 12:{
					op.setOutputFileDirectory("Reducer");
					continue;
				}
				case 13:{
					op.setReducerClass("WordCount.Reducer");
					continue;
				}
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		JobRunner job = new JobRunner();
		long start = System.nanoTime();		
		job.run(spec);
		long end =  System.nanoTime();		
		System.out.println("\nTime taken (millisecs) : " + (end-start)/1000000);
		System.out.println("\nDone!");
	}

}
