package org.ncsu.mapreduce.util;
/* The default Partitioner.It takes the key as the parameter and 
 * using the hashcode value determines which partition/reducer it
 * should go to.
 */
public class HashPartitioner implements Partitioner {

	@Override
	public int assignPartition(Object key,int numReducers) {
		
		int hashCode = key.hashCode();
		int partition = Math.abs(hashCode)%numReducers;
		return partition;		
		
	}

}
