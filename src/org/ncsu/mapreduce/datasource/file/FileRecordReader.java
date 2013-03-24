package org.ncsu.mapreduce.datasource.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import org.ncsu.mapreduce.common.RecordReader;

public class FileRecordReader implements RecordReader {
	private long currentBytePosition; 
	private FileSplitInformation split;
	RandomAccessFile file;
	private int count;
	
	public FileRecordReader(FileSplitInformation split){
		count = 0;
		currentBytePosition = split.getStart();
		this.split = split;
		try {			
			file = new RandomAccessFile(split.getFileInfo().getFileName(), "r");			
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
	
	@Override
	public String getNext(){
		if(currentBytePosition >= split.getStart() + split.getLength())
			return null;
		try {
			byte temp0, temp1='\n', temp2='\r';
		if(currentBytePosition!=0 && count == 0){			
				
				file.seek(currentBytePosition);
				temp0=file.readByte();
				if(temp0 != temp1 && temp0 != temp2){
					currentBytePosition--;
					file.seek(currentBytePosition);
					temp0=file.readByte();
					if(temp0!=temp1 && temp0 != temp2){
						// current mapper has to start reading from next line
						file.readLine();
						currentBytePosition = file.getFilePointer();
					}
					else{ // current mapper has to read from original position
						currentBytePosition++;						
					}					
				}
				else{
					if(temp0 == temp2){
						currentBytePosition++;
						file.seek(currentBytePosition);
						temp0=file.readByte();
					}
					if(temp0 == temp1){
						currentBytePosition++;
						file.seek(currentBytePosition);						
					}
				}
		}
			count++;
			if(currentBytePosition >= split.getStart() + split.getLength())
				return null;
			String temp;			
			file.seek(currentBytePosition);			
			temp  = file.readLine();			
			currentBytePosition = file.getFilePointer();
			return temp;
		} catch (IOException e) {				
			e.printStackTrace();
		}
		return null;		
	}

	public void close() {
		// TODO Auto-generated method stub
		try {
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
