package com.cgi.ppxmqupload;

import java.io.*;
import java.util.Properties;
import java.util.logging.*;
import java.util.Hashtable;
import com.cgi.ppxmqupload.util.XMLFileFilter;
import com.ibm.mq.*; //Include the WebSphere MQ classes for Java package

/**
 * 
 */
public class MQUpload {

	/**
	 *  static variables
	 */
	private static final Logger logger = Logger.getLogger(MQUpload.class.getName());
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

		new MQUpload(args);
	}

	/**
	 * 
	 */
	private static void usage() {
		System.err.println("Usage: java -cp classPath com.cgi.ppxmqupload.MQUpload pathToPropertiesFile");
		System.err.println("\nwhere:");
		System.err.println("\tclassPath is the full or relative path to the necessary jar/zip files for to run the MQUpload");
		System.err.println("\n\tpathToPropertiesFile is the full or relative path to the properties file to be used by the MQUpload");
		System.exit(-1);
	}

	/**
	 * 
	 * @param args
	 */
	public MQUpload(String[] args) {

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
				System.err.println("LOGNAME was not specified in properties file.  Exitting.");
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
			File filt = new File(xmldir);
			FilenameFilter ext = new XMLFileFilter("XML");
			String s[] = filt.list(ext);

			logger.info("You are about to upload "+s.length+" XML file(s).");
			logger.info("Type Y or y then press <return> to proceed OR press <return> to quit.");
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			String inString = null;
			try 
			{
				inString = in.readLine();
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}

			if (inString.equalsIgnoreCase("y"))
			{
				String version="1.1.2";
				logger.info("Running MQUpload version "+version);
				logger.info("Number of XML files found in directory="+s.length);

				// Create a connection to the QueueManager
				//logger.info("Connecting to queue manager: "+qManager);
				logger.info("Connecting to queue manager: "+qManager);
				MQQueueManager qMgr = new MQQueueManager(qManager);

				// Set up the options on the queue we wish to open
				int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;

				// Now specify the queue that we wish to open and the open options
				//logger.info("Accessing queue: "+qName);
				logger.info("Accessing queue: "+qName);
				MQQueue queue = qMgr.accessQueue(qName, openOptions);

				// Define a simple WebSphere MQ Message ...
				MQMessage ppxfile = new MQMessage();
				//Load the String array with the names of all the XML files in the directory

				// For each record, read in XML contents, write to String then output to queue
				for (int i=0; i < s.length; i++) {
					//logger.info("Processing message="+s[i]);
					//Read in contents of XML file to output into String
					File file = new File(xmldir+s[i]);
					StringBuffer contents = new StringBuffer();
					BufferedReader reader = null;
					reader = new BufferedReader(new FileReader(file));
					String text = new String();
					contents.append(reader.readLine());
					while((text = reader.readLine())!= null)
					{
						//contents.append(text); 
						contents.append("\n").append(text);
						//for XML, if linefeeds need to be escaped, used the following
						//contents.append("&#xA;").append(text);
					}//while

					// Specify the default put message options
					//ppxfile.characterSet = 437;
					//ppxfile.encoding = MQC.MQENC_INTEGER_NORMAL;
					//ppxfile.format = "MQSTR";
					ppxfile.format = MQC.MQFMT_STRING;
					MQPutMessageOptions pmo = new MQPutMessageOptions();

					//logger.info("Sending message "+s[i]);
					logger.info("Sending message "+s[i]);
					ppxfile.writeString(contents.toString());
					queue.put(ppxfile, pmo);  // Put the message to the queue
					ppxfile.clearMessage();   //reset message for the next one
				}//for

				// Close the queue
				logger.info("Closing the queue");
				queue.close();

				// Disconnect from the QueueManager
				//logger.info("Disconnecting from the Queue Manager");
				logger.info("Disconnecting from the Queue Manager");
				qMgr.disconnect();
				logger.info("Done!");

			}//if
		}
		catch (MQException ex) {
			logger.severe("A WebSphere MQ Error occured : Completion Code "
					+ ex.completionCode + " Reason Code " + ex.reasonCode);
		}
		catch (java.io.IOException ex) {
			logger.severe("An IOException occured whilst writing to the message buffer: "
					+ ex);
		}
	}//MQUpload

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
				"QCLIENTMODE", "QCHANNEL", "QPORT", "XMLDIR"};

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