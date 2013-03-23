package org.ncsu.mapreduce.common;

import java.util.ArrayList;

import org.ncsu.mapreduce.datasource.file.FileSplitInformation;

public interface InputSplit {	
	ArrayList<FileSplitInformation> getSplits(MapReduceSpecification spec);
}
