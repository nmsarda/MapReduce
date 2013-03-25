package org.ncsu.mapreduce.util;
/* The interface which must be implemented by the partitioners.
 * Default Partitioner is the HashPartitioner.Users can implement
 * their own partitioners by using this interface.
 */
public interface Partitioner {
	int assignPartition(Object key,int numReducers);	
	
}
