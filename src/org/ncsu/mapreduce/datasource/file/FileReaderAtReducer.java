	package org.ncsu.mapreduce.datasource.file;
	
	import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;

import org.ncsu.mapreduce.util.Logger;
	
	/* Reads the reducer files generated by the mappers.
	 * 
	 */
	public class FileReaderAtReducer {
		
		private BufferedReader[] bufferedReader; 
		private int numberOfFiles;//number of reducer input files generated by the mappers
		private String directoryPath;
		private String currentLine[];
		
		public FileReaderAtReducer(int reducerId){
			directoryPath = "ReducerData" + System.getProperty("file.separator") + "Reducer_" + reducerId + System.getProperty("file.separator");
			numberOfFiles = new File(directoryPath).list().length;
			currentLine = new String[numberOfFiles];
			for(int i=0; i<numberOfFiles; i++){
				currentLine[i]="";
			}
			bufferedReader = new BufferedReader[numberOfFiles];
			initializeFileReaders();
		}
		/* Initialize a buffered reader per the number of input files
		 * generated by the mappers
		 */
		void initializeFileReaders(){
			String[] fileNames = new File(directoryPath).list();
			for(int i=0; i<numberOfFiles;i++){			
				try {
					bufferedReader[i] = new BufferedReader(new FileReader(directoryPath + fileNames[i]));
				} catch (FileNotFoundException e) {				
					Logger.getLogger().log(Level.SEVERE,e.getMessage());
				}			
			}		
		}
		/* Read a line from each of the input files.Find the minimum key
		 * and aggregate the tuples and send it to the ReduceRunner.
		 */
		public String getNext(){
			String line;
			for(int i=0; i< numberOfFiles; i++){
				if(currentLine[i]!=null && currentLine[i].equals("")){
					try {
						currentLine[i] = bufferedReader[i].readLine();
					} catch (IOException e) {					
						Logger.getLogger().log(Level.SEVERE,e.getMessage());
					}
				}
			}
			int minIndex = getMin();	
			if(minIndex == -1){
				return null; 
			}
			line = currentLine[minIndex];
			currentLine[minIndex] = "";
			return line;
		}
		/* Get the minimum key from all the files
		 * 
		 */
		private int getMin(){
			int minIndex = -1;
			String min = null;
			for(int i=0; i< numberOfFiles; i++){
				if(currentLine[i]!=null){
					if(min == null){					
						min = currentLine[i].split(" ")[0];
						minIndex = i;					
					}
					else{
						if(currentLine[i].split(" ")[0].compareToIgnoreCase(min) < 0){
							min = currentLine[i].split(" ")[0];
							minIndex = i;
						}
					}
				}		
			}
			return minIndex;
		}
		/* Close the buffered readers and the files.
		 * 
		 */
		public void close()
		{
			for(int i=0; i<numberOfFiles;i++)
			{			
				try
				{
					bufferedReader[i].close();
				} 
				catch (IOException e) 
				{				
					Logger.getLogger().log(Level.SEVERE,e.getMessage());
				}
			}
		}
	}
