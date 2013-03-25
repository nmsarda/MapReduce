package org.ncsu.mapreduce.common;

import java.util.ArrayList;

/*
 * User's class which implements map method must inherit this class. 
 */

public interface Mapper<K extends Comparable<K>, V>  {
	
	public ArrayList<KeyValueClass<K, V>> map(K mapInput);

}
