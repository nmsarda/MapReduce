package org.ncsu.mapreduce.util;

public interface Partitioner<K> {
	int assignPartition(K key,int numReducers);	
	
}
