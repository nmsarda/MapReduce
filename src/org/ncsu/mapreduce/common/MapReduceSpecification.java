package org.ncsu.mapreduce.common;

/*
 * 
 * This class determines various specifications required to run MapReduce. This includes
 * Input and Output specifications.
 * 
 */

public class MapReduceSpecification {
	private int noOfThreads;  // Determines total number of threads to be used.
	private int noOfMappers;  // Determines total number of mappers.
	private int noOfReducers; // Determines total number of reducers.
	private int minByteSize;  // Determines byte size to be used per split of data	 
	private MapReduceInput inp; // MapReduceInput object
	private MapReduceOutput mapReduceOutputClass; // MapReduceOutput object
	
	public MapReduceSpecification(){
		noOfThreads = 10;
		minByteSize = 1024;	
		noOfMappers = 0;
		noOfReducers = 0;
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

	public MapReduceOutput output(){
		mapReduceOutputClass = new MapReduceOutput();
		return mapReduceOutputClass;
	}
	public double getNoOfMappers() {		
		return noOfMappers;
	}
	
	public int getNoOfReducers() {		
		return noOfReducers;
	}

	
	public MapReduceOutput getMapReduceOutputClass() {
		return mapReduceOutputClass;
	}

	
	public void setMapReduceOutputClass(MapReduceOutput mapReduceOutputClass) {
		this.mapReduceOutputClass = mapReduceOutputClass;
	}
	public void calculateThreads()
	{
		if(noOfMappers == 0 && noOfReducers == 0)
		{
			setNoOfMappers((int)Math.ceil(0.75*noOfThreads));
			setNoOfReducers(noOfThreads-noOfMappers);
		}
		else if(noOfMappers == 0)
		{
			int threads = (int)Math.ceil(noOfReducers/0.25);
			setNoOfThreads(threads);
			setNoOfMappers(threads - noOfReducers);
			
		}
		else if(noOfReducers == 0)
		{
			int threads = (int)Math.ceil(noOfMappers/0.75);
			setNoOfThreads(threads);
			setNoOfReducers(threads - noOfMappers);
		}
		System.out.println("Reducers "+noOfReducers + "Mappers" + noOfMappers );
	}
}
