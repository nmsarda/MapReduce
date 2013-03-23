package org.ncsu.mapreduce.util;

public class HashPartitioner<K> implements Partitioner<K> {

	@Override
	public int assignPartition(K key,int numReducers) {
		
		int hashCode = key.hashCode();
		int partition = hashCode%numReducers;
		return partition;		
		
	}

}
