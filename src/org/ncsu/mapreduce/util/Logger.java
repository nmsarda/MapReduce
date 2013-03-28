package org.ncsu.mapreduce.util;

import java.io.IOException;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import javax.sound.sampled.Line;

/* Custom class to manage logs 
 * 
 */
public class Logger {
	
	private static Level logLevel = Level.SEVERE;
	private static java.util.logging.Logger logObject;
	private static FileHandler handler;
	private static TextFormatter formatter;
	private static Logger logger=null;
	private Logger(){}
	public static Logger getLogger()
	{
		if(logger != null)
			return logger;
		else
		{
			logger = new Logger();
			try {
					handler = new FileHandler("MapReduce.log");
				
			} catch (SecurityException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			formatter =  new TextFormatter();
			handler.setFormatter(formatter);
			logObject =  java.util.logging.Logger.getLogger("MapReduceLogger");
			logObject.setLevel(logLevel);
			logObject.addHandler(handler);
			return logger;
		}
		
	}
	public void setLogLevel(Level logLevel)
	{
		Logger.logLevel = logLevel;
		if(logObject != null)
		{
			logObject.setLevel(logLevel);
		}
	}
	public Level getLogLevel()
	{
		return logLevel;
	}
	public void log(Level level, String message)
	{
		LogRecord logRecord = new LogRecord(level,message);
		logObject.log(level, message);
		
	}
	public void log(Level level,String message,Formatter format)
	{
		/*
		 * to be implemented
		 */
	}

}

class TextFormatter extends Formatter
{
   public String format(LogRecord log) 
   {
      Date date = new Date(log.getMillis());
      String level = log.getLevel().getName();
      String logMessage = "MapReduce:["+level+" "+date.toString()+"] ";
      logMessage = logMessage + log.getMessage()+System.getProperty("line.separator");
      System.out.println(logMessage);
         
      Throwable thrown = log.getThrown();
      if (thrown != null) { 
         logMessage = logMessage + thrown.toString(); 
      }
      return logMessage;
   }


}
