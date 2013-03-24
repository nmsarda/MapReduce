package org.ncsu.mapreduce.util;

public class HashPartitioner implements Partitioner {

	@Override
	public int assignPartition(Object string,int numReducers) {
		
		int hashCode = string.hashCode();
		int partition = Math.abs(hashCode)%numReducers;
		return partition;		
		
	}

}
