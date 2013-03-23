package org.ncsu.mapreduce.util;

public class Logger {
	
	private static Logger logger=null;
	private Logger(){}
	public static Logger getLogger()
	{
		if(logger == null)
			return logger;
		else
		{
			logger = new Logger();
			return logger;
		}
		
	}

}
