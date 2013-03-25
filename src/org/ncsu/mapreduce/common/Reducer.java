package org.ncsu.mapreduce.common;

import java.util.ArrayList;

/*
 * User's class which implements reduce method must inherit this class. 
 */

public interface Reducer {
	public String Reduce(String key, ArrayList<String> values);
}
