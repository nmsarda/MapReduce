package org.ncsu.mapreduce.util;

public interface Partitioner {
	int assignPartition(Object key,int numReducers);

	//int assignPartition();	
	
}
