package com.cgi.t360.util;

import java.io.IOException;
import java.util.logging.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogMgr {

	private static final Logger logger = Logger.getLogger(LogMgr.class.getName());
	private static FileHandler fh = null;
	private static LogManager lm = LogManager.getLogManager();
	private static String logFilename = null;
	private static int logLevel;
	
	/**
	 * 
	 * @param pm
	 */
	public LogMgr(PropertiesManager pm)
	{
		try {
			logFilename = pm.getProperty("logFilename");
			if(logFilename == null)
			{
				System.err.println("[CIALogManager] Log filename (logFilename) was not specified in the properties file");
				System.exit(-1);
			}
			else
			{
				fh = new FileHandler(pm.getProperty("logFilename"));
				fh.setFormatter(new SimpleFormatter());
				lm.addLogger(logger);	
				logger.addHandler(fh);
				
				String logLevelString = pm.getProperty("logLevel");
				if(logLevelString == null)
				{
					System.err.println("[CIALogManager] Log level (logLevel) was not specified in the properties file");
					System.exit(-1);
				}
				else
				{
					if(logLevelString.equals(Level.INFO.toString()))
					{
						logLevel = 0;
					}
					else if(logLevelString.equals(Level.WARNING.toString()))
					{
						logLevel = 1;
					}
					else if(logLevelString.equals(Level.SEVERE.toString()))
					{
						logLevel = 2;
					}
					else
					{
						System.err.println("[CIALogManager] Unknown value for log level was specified in the properties file: "+logLevelString);
						System.exit(-1);
					}
				}
			}
		} catch (SecurityException e) {
			System.err.println("[CIALogManager] SecurityException caught while initializing error logfile");
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			System.err.println("[CIALogManager] IOException caught while initializing error logfile");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * 
	 * @param className
	 * @param message
	 */
	public void logInfoToFile(Object classObject, String message)
	{		
		if(logLevel < 1)
		{
			logToFile(classObject.getClass().getName(), Level.INFO, message);
		}
	}
	
	/**
	 * 
	 * @param classObject
	 * @param message
	 */
	public void logWarningToFile(Object classObject, String message)
	{
		if(logLevel < 2)
		{
			logToFile(classObject.getClass().getName(), Level.WARNING, message);
		}
	}
	
	/**
	 * 
	 * @param classObject
	 * @param message
	 */
	public void logErrorToFile(Object classObject, String message)
	{
		if(logLevel <= 2)
		{
			logToFile(classObject.getClass().getName(), Level.SEVERE, message);
		}
	}
	
	/**
	 * 
	 * @param className
	 * @param message
	 */
	public void logInfo(Object classObject, String message)
	{	
		if(logLevel < 1)
		{
			logToFile(classObject.getClass().getName(), Level.INFO, message);			
		}
	}
	
	/**
	 * 
	 * @param classObject
	 * @param message
	 */
	public void logWarning(Object classObject, String message)
	{
		if(logLevel < 2)
		{
			logToFile(classObject.getClass().getName(), Level.WARNING, message);
		}
	}
	
	/**
	 * 
	 * @param classObject
	 * @param message
	 */
	public void logError(Object classObject, String message)
	{
		if(logLevel <= 2)
		{
			logToFile(classObject.getClass().getName(), Level.SEVERE, message);
		}
	}
	
	/**
	 * 
	 * @param className
	 * @param level
	 * @param message
	 */
	private void logToFile(String className, Level level, String message)
	{		
		if(level == Level.INFO)
		{
			logger.setLevel(Level.INFO);
			logger.info("["+className+"] "
					+message);
		}
		else if(level == Level.WARNING)
		{
			logger.setLevel(Level.WARNING);
			logger.warning("["+className+"] "
					+message);			
		}
		else if(level == Level.SEVERE)
		{
			logger.setLevel(Level.SEVERE);
			logger.severe("["+className+"] "
					+message);
		}
	}

	/**
	 * 
	 * @return
	 */
	private String getCurrentTimestamp()
	{
		Date now = new Date();
		SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		return simpleFormat.format(now);
	}
}
