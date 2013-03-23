package org.ncsu.mapreduce.common;

public class MapReduceSpecification {
	private int noOfThreads;
	private int noOfMappers;
	private int minByteSize;
	private int noOfReducers;
	private Class<?> OutputKeyformat;
	private Class<?> OutputValueformat;
	private MapReduceInput inp;	
	
	public Class<?> getOutputKeyformat() {
		return OutputKeyformat;
	}

	public void setOutputKeyformat(Class<?> outputKeyformat) {
		OutputKeyformat = outputKeyformat;
	}

	public Class<?> getOutputValueformat() {
		return OutputValueformat;
	}

	public void setOutputValueformat(Class<?> outputValueformat) {
		OutputValueformat = outputValueformat;
	}


	
	public MapReduceSpecification(){
		noOfThreads = 5;
		minByteSize = 10;	
		noOfMappers = 3;
	}
	
	public void setNoOfThreads(int noOfThreads)
	{
		this.noOfThreads = noOfThreads;				
	}
	
	public void setNoOfMappers(int noOfMappers)
	{
		this.noOfMappers = noOfMappers;				
	}
	
	public void setNoOfReducers(int noOfReducers)
	{
		this.noOfReducers = noOfReducers;				
	}
	
	public void minByteSize(int minByteSize){
		this.minByteSize = minByteSize;
	}
	
	public int getNoOfThreads(){
		return noOfThreads;
	}
	
	public int getMinByteSize(){
		return minByteSize;
	}
	
	public MapReduceInput getMapReduceInput(){
		return inp;
	}
	
	public MapReduceInput add_input(){
		inp = new MapReduceInput();
		return inp;
	}

	public double getNoOfMappers() {		
		return noOfMappers;
	}
	
	public int getNoOfReducers() {		
		return noOfReducers;
	}
}
