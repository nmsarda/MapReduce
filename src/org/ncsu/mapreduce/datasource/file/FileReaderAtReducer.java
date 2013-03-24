	package org.ncsu.mapreduce.datasource.file;
	
	import java.io.BufferedReader;
	import java.io.File;
	import java.io.FileNotFoundException;
	import java.io.FileReader;
	import java.io.IOException;
	
	
	public class FileReaderAtReducer {
		
		private BufferedReader[] bufferedReader;// = new BufferedReader(new FileReader("abc.txt")); 
		private int numberOfFiles;
		private int reducerId;
		private String directoryPath;
		private String currentLine[];
		public FileReaderAtReducer(int reducerId){
			this.reducerId = reducerId;
			directoryPath = "ReducerData" + System.getProperty("file.separator") + "Reducer_" + reducerId + System.getProperty("file.separator");
			numberOfFiles = new File(directoryPath).list().length;
			currentLine = new String[numberOfFiles];
			for(int i=0; i<numberOfFiles; i++){
				currentLine[i]="";
			}
			bufferedReader = new BufferedReader[numberOfFiles];
			initializeFileReaders();
		}
		
		void initializeFileReaders(){
			String[] fileNames = new File(directoryPath).list();
			for(int i=0; i<numberOfFiles;i++){			
				try {
					bufferedReader[i] = new BufferedReader(new FileReader(directoryPath + fileNames[i]));
				} catch (FileNotFoundException e) {				
					e.printStackTrace();
				}			
			}		
		}
	
		public String getNext(){
			String line;
			for(int i=0; i< numberOfFiles; i++){
				if(currentLine[i]!=null && currentLine[i].equals("")){
					try {
						currentLine[i] = bufferedReader[i].readLine();
					} catch (IOException e) {					
						e.printStackTrace();
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
		
		public int getMin(){
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
	}
