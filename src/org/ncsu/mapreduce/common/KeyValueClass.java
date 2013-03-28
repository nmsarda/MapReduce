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
	return this.key.compareTo(o.key);
}

}


