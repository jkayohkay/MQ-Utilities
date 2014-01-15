package com.cgi.ppxmqupload;

import java.io.*;
import java.util.Properties;
import java.util.logging.*;
import java.util.Hashtable;
import com.ibm.mq.*; //Include the WebSphere MQ classes for Java package

/**
 * 
 */
public class MQDownload {

	/**
	 *  static variables
	 */
	private static final Logger logger = Logger.getLogger(MQDownload.class.getName());
	private static FileHandler fh;
	private static LogManager lm;
	private static Properties prop=new Properties();

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if((args.length == 0)
				|| (args[0].isEmpty()))
		{
			usage();
		}

		new MQDownload(args);
	}

	/**
	 * 
	 */
	private static void usage() {
		System.err.println("Usage: java -cp classPath com.cgi.ppxMQDownload.MQDownload pathToPropertiesFile");
		System.err.println("\nwhere:");
		System.err.println("\tclassPath is the full or relative path to the necessary jar/zip files for to run the MQDownload");
		System.err.println("\n\tpathToPropertiesFile is the full or relative path to the properties file to be used by the MQDownload");
		System.exit(-1);
	}

	/**
	 * 
	 * @param args
	 */
	public MQDownload(String[] args) {

		try {
			prop.load(new FileInputStream(args[0]));

			//logging
			lm = LogManager.getLogManager();
			if(prop.getProperty("LOGNAME") != null)
			{
				fh=new FileHandler(prop.getProperty("LOGNAME"), true);
				lm.addLogger(logger);
				logger.setLevel(Level.INFO);
				fh.setFormatter(new SimpleFormatter());
				logger.addHandler(fh);
			}
			else
			{
				System.err.println("LOGNAME was not specified in properties file.  Exiting.");
				System.exit(-1);
			}

			// Load the rest of the properties from the properties file
			Hashtable<String,String> properties = loadProperties(args[0], prop, logger);

			// define the name of the QueueManager
			String qManager = properties.get("QMANAGER");

			// define the name of the Queue
			String qName = properties.get("QNAME");

			// define the name of your host to connect to
			// Set property QCLIENTMODE to Y if connecting remotely
			if(properties.get("QCLIENTMODE").equalsIgnoreCase("Y")) 
			{
				logger.info("Setting MQ connection as Client mode");		

				// Set up WebSphere MQ environment
				MQEnvironment.hostname = properties.get("QHOSTNAME");
				MQEnvironment.channel  = properties.get("QCHANNEL");
				MQEnvironment.port  = Integer.parseInt(properties.get("QPORT"));
				
				MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT);
			}
			else
			{
				logger.info("Setting MQ connection as Server mode");		
				// Server Connection
				MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,  MQC.TRANSPORT_MQSERIES);
			}

			// define the PPX XML directory
			String xmldir = ((args.length > 1) && (args[1] != null) && (!args[1].isEmpty())) ? args[1] : properties.get("XMLDIR");
			String version="1.1.2";
			logger.info("Running MQDownload version "+version);

			// Create a connection to the QueueManager
			//logger.info("Connecting to queue manager: "+qManager);
			logger.info("Connecting to queue manager: "+qManager);
			MQQueueManager qMgr = new MQQueueManager(qManager);

			// Set up the options on the queue we wish to open
			//int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;
			int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT | MQC.MQOO_INQUIRE;
			// Now specify the queue that we wish to open and the open options
			logger.info("Accessing queue: "+qName);
			MQQueue queue = qMgr.accessQueue(qName, openOptions);

			int msgCount=queue.getCurrentDepth();
			//check queue depth
			logger.info("Number of messages found in queue="+msgCount);
			
			String filePrefix = properties.get("OUTPUTNAME");
			String fileExtension = properties.get("EXTENSION");
			logger.info("filePrefix="+filePrefix);
			logger.info("fileExtension="+fileExtension);

			// For each message in queue, write to file
			for (Integer i=0; i < msgCount; i++) {
				//logger.info("Current count="+i);
				StringBuffer fileName = new StringBuffer();

				// Define a simple WebSphere MQ Message ...
				MQMessage ppxfile = new MQMessage();
	
				MQGetMessageOptions gmo = new MQGetMessageOptions();
	
				// Get a message from the queue
				queue.get(ppxfile,gmo);
	
				//Extract the message data
				int len=ppxfile.getDataLength();
				byte[] message = new byte[len];
				ppxfile.readFully(message,0,len);
				String msgString = new String(message);
				
				//StringBuffer fileName = filePrefix.append()
				fileName.append(xmldir).append(filePrefix).append(i.toString()).append(".").append(fileExtension);								

				BufferedWriter out = new BufferedWriter(new FileWriter(fileName.toString()));
				out.write(msgString);
				out.close();

			}//for

			// Close the queue
			logger.info("Closing the queue");
			queue.close();

			// Disconnect from the QueueManager
			//logger.info("Disconnecting from the Queue Manager");
			logger.info("Disconnecting from the Queue Manager");
			qMgr.disconnect();
			logger.info("Done!");


		}
		catch (MQException ex) {
			logger.severe("A WebSphere MQ Error occured : Completion Code "
					+ ex.completionCode + " Reason Code " + ex.reasonCode);
		}
		catch (java.io.IOException ex) {
			logger.severe("An IOException occured whilst writing to file: "
					+ ex);
		}
	}//MQDownload

	/**
	 * 
	 * @param propFilename
	 * @param prop
	 * @param logger
	 * @return
	 */
	private Hashtable<String,String> loadProperties(String propFilename, Properties prop, Logger logger)
	{
		String[] propertyNames = {"QMANAGER", "QNAME", "QHOSTNAME", 
				"QCLIENTMODE", "QCHANNEL", "QPORT", "XMLDIR", "OUTPUTNAME", "EXTENSION"};

		Hashtable<String,String> returnHt = new Hashtable<String,String>();
		for(int i=0; i < propertyNames.length; i++)
		{
			if(prop.getProperty(propertyNames[i]) != null)
			{
				returnHt.put(propertyNames[i], prop.getProperty(propertyNames[i]));
			}
			else
			{
				logger.severe(propertyNames[i]+" was not specified in properties file("+propFilename+").  Unable to continue.");
				System.exit(-1);
			}
		}

		return returnHt;
	}
}