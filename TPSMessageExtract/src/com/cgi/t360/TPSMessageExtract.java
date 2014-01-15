package com.cgi.t360;

import com.cgi.t360.util.*;
import java.io.*;
import java.util.*;

public class TPSMessageExtract {

	private static DBManager dbMgr = null;
	private static LogMgr logMgr = null;
	private static PropertiesManager propMgr = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new TPSMessageExtract(args);
	}

	TPSMessageExtract(String[] args)
	{
		// Initialize stuff
		propMgr = new PropertiesManager(args[0]);
		logMgr = new LogMgr(propMgr);
		propMgr.setLogManager(logMgr);
		dbMgr = new DBManager(logMgr, propMgr);

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in)); 
		
		try
		{
			System.out.println("[OPTIONAL] Enter business date (in the format MM/DD/YY) to limit extract or blank line to include all dates: ");
			String businessDate = reader.readLine();
			businessDate = parseBusinessDate(businessDate);
			
			System.out.println("Found the following message types: ");

			Hashtable<String, String> listOfMsgTypes = dbMgr.getListOfMessageTypes(businessDate);
			int totalNumberTypes = listOfMsgTypes.size();
			for(int i = 1; i <= totalNumberTypes; i++)
			{
				System.out.println("["+String.valueOf(i)+"]\t"+listOfMsgTypes.get(String.valueOf(i)));
			}

			System.out.println("\nPlease enter the number of the type you'd like to extract (just one number please)");
			String userSelection = reader.readLine();

			if((userSelection != null) && (!userSelection.isEmpty()))
			{
				if(listOfMsgTypes.containsKey(userSelection.trim()))
				{
					// Now that the user has made a selection, go get the stuff and write them to files
					Hashtable<String,String> xmlUoids = dbMgr.getXmlUoids(listOfMsgTypes.get(userSelection), businessDate);

					if(xmlUoids != null)
					{
						int filesWritten = 0;
						int filesNotWritten = 0;
						Enumeration<String> keys = xmlUoids.keys();
						while(keys.hasMoreElements())
						{
							String messageUoid = keys.nextElement();
							String wpUoid = xmlUoids.get(messageUoid);
							String xml = dbMgr.getXml(wpUoid);
							
							if(xml != null)
							{
								filesWritten++;
								logMgr.logInfo(this, "Writing XML to file for message uoid = "+messageUoid);
								writeToFile(listOfMsgTypes.get(userSelection).trim(), messageUoid.trim(), xml);
							}
							else
							{
								filesNotWritten++;
								logMgr.logError(this, "No XML to write for message uoid = "+messageUoid);
							}
						}
						
						logMgr.logInfo(this, "Wrote "+filesWritten+" files");
						logMgr.logInfo(this, "Unable to write "+filesNotWritten+" files");					
					}
				}
				else
				{
					System.err.println("Invalid selection entered.");
					System.exit(-1);
				}
			}
			else
			{
				System.err.println("Blank value entered.  Nothing to do.");
				System.exit(-1);
			}
		}
		catch (IOException e)
		{
			System.err.println("IOException caught while trying to read in user selection");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private String parseBusinessDate(String businessDate) {
		businessDate = businessDate.trim();
		
		if((businessDate == null)
			|| (businessDate.isEmpty()))
		{
			return new String();
		}
		else if(businessDate.length() != 8)
		{
			System.err.println("Invalid business date entered; length is not 8");
			System.exit(-1);
		}
		else 
		{
			StringTokenizer tokens = new StringTokenizer(businessDate, "/");
			if(tokens.countTokens() != 3)
			{
				System.err.println("Invalid business date entered: did not find 3 tokens delimited by '/'");
				System.exit(-1);
			}
			else
			{
				String month = tokens.nextToken();
				String day = tokens.nextToken();
				String year = tokens.nextToken();
				
				if(month.equals("01"))
					month = "JAN";
				else if(month.equals("02"))
					month = "FEB";
				else if(month.equals("03"))
					month = "MAR";
				else if(month.equals("04"))
					month = "APR";
				else if(month.equals("05"))
					month = "MAY";
				else if(month.equals("06"))
					month = "JUN";
				else if(month.equals("07"))
					month = "JUL";
				else if(month.equals("08"))
					month = "AUG";
				else if(month.equals("09"))
					month = "SEP";
				else if(month.equals("10"))
					month = "OCT";
				else if(month.equals("11"))
					month = "NOV";
				else if(month.equals("12"))
					month = "DEC";
				else
				{
					System.err.println("Invalid business date entered; Unknown month entered");
					System.exit(-1);
				}
				
				return day+"-"+month+"-"+year;
			}
		}
		return new String();
	}

	private void writeToFile(String msgType, String uoid, String stringToWrite)
	{
		String filename = propMgr.getProperty("outputPath")+File.separator+msgType.trim()+"-"+uoid+".xml";

		logMgr.logInfo(this, "Writing "+filename);

		try
		{
			BufferedWriter out = new BufferedWriter(new FileWriter(filename));
			out.write(stringToWrite);
			out.close(); 
		}
		catch (IOException e)
		{
			logMgr.logError(this, "IOException caught while trying to write to file for msgType = "+msgType.trim()+" and uoid = "+uoid);
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
