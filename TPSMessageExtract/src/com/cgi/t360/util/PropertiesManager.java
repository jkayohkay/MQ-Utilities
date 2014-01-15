package com.cgi.t360.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertiesManager {
	private static Properties prop = new Properties();
	private static LogMgr lm = null;
	private static String propFilename = null;
	
	/**
	 * 
	 */
	public PropertiesManager()
	{
		new PropertiesManager("TPSMessageExtract.properties");
	}
	
	/**
	 * 
	 * @param propertiesFilename
	 */
	public PropertiesManager(String propertiesFilename)
	{
		propFilename = propertiesFilename;
		
		// Default to the current working directory
		FileInputStream fileInputStream = null;		
		try {
			fileInputStream = new FileInputStream(propFilename); 
			prop.load(fileInputStream);
		}
		catch (FileNotFoundException e)
		{
			System.err.println("[PropertiesManager] "
					+"Caught FileNotFoundException when trying to load properties file for TPSMessageExtract ("+propFilename+")");
			e.printStackTrace();
			System.exit(-1);
		}
		catch (IOException e)
		{
			System.err.println("[PropertiesManager] "
					+"Caught IOException when trying to load properties file for TPSMessageExtract ("+propFilename+")");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void setLogManager(LogMgr logManager)
	{
		if(logManager != null)
		{
			lm = logManager;
		}
		else
		{
			System.err.println("[PropertiesManager.setLogManager(LogMgr)] "
					+"Reference to LogMgr is null.  Unable to set the PropertiesManager's LogMgr");
			System.exit(-1);
		}
	}
	
	/**
	 * 
	 * @param propertyName
	 * @return
	 */
	public String getProperty(String propertyName)
	{
		String propertyValue = prop.getProperty(propertyName);
		
		if(propertyValue == null)
		{
			lm.logWarning(this, "The following propertyName is missing from "
					+propFilename
					+": "+propertyName);
			return null;
		}
		else if(propertyValue.isEmpty())
		{
			lm.logWarning(this, "The following propertyName is blank in "
					+propFilename
					+": "+propertyName);
			return null;
		}
		else
		{
			return propertyValue;
		}
	}
}
