package org.ncsu.mapreduce.datasource.file;
/*
 * This interface when implemented returns a iterator over the input split given by
 * the inputSplitter.
 */
public interface RecordReader {
	String getNext();	// Returns next line
}	
