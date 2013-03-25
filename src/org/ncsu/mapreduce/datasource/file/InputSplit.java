package org.ncsu.mapreduce.datasource.file;

import java.util.ArrayList;

import org.ncsu.mapreduce.common.MapReduceSpecification;

/*
 * This interface provides getSplits method which splits the input files.
 * By default it splits taking into factors like number of mappers, maximum block size
 * and total file size
 */

public interface InputSplit {	
	ArrayList<FileSplitInformation> getSplits(MapReduceSpecification spec);
	/*
	 * spec contains all specifications mentioned by the user for running the program
	 * This method returns an arrayList of FileSplitInformation objects which contains
	 * information of individual splits like file information, split start, split size.
	 */
}
