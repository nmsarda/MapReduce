package org.ncsu.mapreduce.datasource.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;

import org.ncsu.mapreduce.util.Logger;

/* Reads the records from the file. Reading is done as per the following scheme
 * Continue reading till the split size and continue reading till the 
 * next newline.
 */
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
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		}
	}
	
	@Override
	/* Records are read in such a manner that each mapper at the beginning of
	 * the read considers the following:
	 * 1) If this byte has been read by a different mapper. This is done by
	 * proceeding to the next newline from the split start.
	 */
	
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
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		}
		return null;		
	}
	/* Close the input file.
	 * 
	 */
	public void close() {
		// TODO Auto-generated method stub
		try {
			file.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Logger.getLogger().log(Level.SEVERE,e.getMessage());
		}
		
	}
}
