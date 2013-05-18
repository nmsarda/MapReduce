package org.ncsu.mapreduce.common;

import java.util.Comparator;

/*
 * 
 * This class stores Key-Value pairs from the map function
 * 
 */
public class KeyValueClass<K extends Comparable<K>, V> implements Comparable<KeyValueClass<K, V>>{
	private K key;
	private V value;
	public KeyValueClass(K key, V value){
this.key = key;
this.value = value;
}

	public K getKey() {
return key;
}

	public void setKey(K key) {
this.key = key;
}

	public V getValue() {
return value;
}

	public void setValue(V value) {
this.value = value;
}

@Override
	public int compareTo(KeyValueClass<K, V> o) {
	if (o.key instanceof String)
	{
		String key1 = (String)o.key;
		String key2 = (String)this.key;
		return key2.compareToIgnoreCase(key1);
		
	}
	else
	return this.key.compareTo(o.key);
	
		
}

}


