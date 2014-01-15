package com.cgi.t360.util.mq;

import java.io.*;
import java.util.Properties;
import java.util.logging.*;
import java.util.Date;
import java.text.SimpleDateFormat;


import com.cgi.t360.util.IsFileFilter;
import com.ibm.mq.*; //Include the WebSphere MQ classes for Java package

public class InterfaceMQUpload {

	private static final Logger logger = Logger.getLogger(InterfaceMQUpload.class.getName());
	private static FileHandler fh;
	private static LogManager lm;
	private static Properties prop=new Properties();

	public static void main(String args[]) {
		
		if((args.length == 0)
				|| (args[0].isEmpty()))
		{
			usage();
		}
		
		new InterfaceMQUpload(args);
	}
	
	private static void usage() {
		System.err.println("Usage: java -cp classPath com.cgi.ppxmqupload.InterfaceMQUpload pathToPropertiesFile");
		System.err.println("\nwhere:");
		System.err.println("\tclassPath is the full or relative path to the necessary jar/zip files for to run the InterfaceMQUpload");
		System.err.println("\n\tpathToPropertiesFile is the full or relative path to the properties file to be used by the InterfaceMQUpload");
		System.exit(-1);
	}

	public InterfaceMQUpload(String[] args) {

		try 
		{
			prop.load(new FileInputStream(args[0]));

			//logging
			lm = LogManager.getLogManager();
			fh=new FileHandler(prop.getProperty("logName"), true);
			lm.addLogger(logger);
			logger.setLevel(Level.INFO);
			fh.setFormatter(new SimpleFormatter());
			logger.addHandler(fh);

			int runInterval = Integer.parseInt(prop.getProperty("runInterval"));

			String version="1.1.4";
			logger.info("Running InterfaceMQUpload version "+version);


			//Load Interface1 Parameters
			String interfaceName1  = prop.getProperty("interfaceName1");
			String qManager1 = prop.getProperty("qManager1");
			String qName1 = prop.getProperty("qName1");
			String qHostname1 = null;
			int qPort1 = 0;
			String qChannel1 = null;

			if(prop.getProperty("qClientMode1").equals("Y")) 
			{
				logger.info("Setting MQ connection for "+interfaceName1+" as Client mode");
				qHostname1 = prop.getProperty("qHostname1");
				qPort1 = Integer.parseInt(prop.getProperty("qPort1"));
				qChannel1 = prop.getProperty("qChannel1");
			}
			else
			{
				logger.info("Setting MQ connection for "+interfaceName1+" as Server mode");
			}
			
			String fileDir1 = prop.getProperty("fileDir1");
			String archiveDir1 = prop.getProperty("archiveDir1");

			//Load Interface2 Parameters
			String interfaceName2  = prop.getProperty("interfaceName2");
			String qManager2 = prop.getProperty("qManager2");
			String qName2 = prop.getProperty("qName2");
			String qHostname2 = null;
			int qPort2 = 0;
			String qChannel2 = null;
			
			if(prop.getProperty("qClientMode2").equals("Y")) 
			{
				logger.info("Setting MQ connection for "+interfaceName2+" as Client mode");
				qHostname2 = prop.getProperty("qHostname2");
				qPort2 = Integer.parseInt(prop.getProperty("qPort2"));
				qChannel2 = prop.getProperty("qChannel2");
			}
			else
			{
				logger.info("Setting MQ connection for "+interfaceName2+" as Server mode");
			}

			String fileDir2 = prop.getProperty("fileDir2");
			String archiveDir2 = prop.getProperty("archiveDir2");

			//Load Interface3 Parameters
			String interfaceName3  = prop.getProperty("interfaceName3");
			String qManager3 = prop.getProperty("qManager3");
			String qName3 = prop.getProperty("qName3");
			String qHostname3 = null;
			int qPort3 = 0;
			String qChannel3 = null;
			
			if(prop.getProperty("qClientMode3").equals("Y")) 
			{
				logger.info("Setting MQ connection for "+interfaceName3+" as Client mode");
				qHostname3 = prop.getProperty("qHostname3");
				qPort3 = Integer.parseInt(prop.getProperty("qPort3"));
				qChannel3 = prop.getProperty("qChannel3");
			}
			else
			{
				logger.info("Setting MQ connection for "+interfaceName3+" as Server mode");
			}

			String fileDir3 = prop.getProperty("fileDir3");
			String archiveDir3 = prop.getProperty("archiveDir3");

			//Load MISDLR (Interface4) Parameters if conversion is active.
			boolean convertMISDLR = false;
			String interfaceName4  = prop.getProperty("interfaceName4");
			String qManager4 = prop.getProperty("qManager4");
			String qName4 = prop.getProperty("qName4");
			String qHostname4 = null;
			int qPort4 = 0;
			String qChannel4 = null;
			
			if(prop.getProperty("qClientMode4").equals("Y")) 
			{
				logger.info("Setting MQ connection for "+interfaceName4+" as Client mode");
				qHostname4 = prop.getProperty("qHostname4");
				qPort4 = Integer.parseInt(prop.getProperty("qPort4"));
				qChannel4 = prop.getProperty("qChannel4");
			}
			else
			{
				logger.info("Setting MQ connection for "+interfaceName4+" as Server mode");
			}

			String fileDir4 = prop.getProperty("fileDir4");
			String archiveDir4 = prop.getProperty("archiveDir4");
			if(prop.getProperty("MISDLR_Convert_Flag").equalsIgnoreCase("TRUE")) 
				convertMISDLR = true;
			
			// set up repeating interval here
			while (true) {
				uploadToMQ (qManager1, qName1, qHostname1, qChannel1, qPort1, fileDir1, archiveDir1, interfaceName1);
				uploadToMQ (qManager2, qName2, qHostname2, qChannel2, qPort2, fileDir2, archiveDir2, interfaceName2);
				uploadToMQ (qManager3, qName3, qHostname3, qChannel3, qPort3, fileDir3, archiveDir3, interfaceName3);				
				if (convertMISDLR)
					uploadToMQ (qManager4, qName4, qHostname4, qChannel4, qPort4, fileDir4, archiveDir4, interfaceName4);
				try {
					java.lang.Thread.currentThread().sleep(runInterval*1000);
				} catch (Exception e) {
					logger.severe("Caught the following Exception while trying sleep: "+e.getMessage());
					System.exit(-1);
				}
			} //while(true) loop
		}  //try
		catch (IOException ex) 
		{
			logger.severe("An IOException occured whilst writing to the message buffer: "
					+ ex.getMessage());
			System.exit(-1);
		}
	}//InterfaceMQUpload


	public void uploadToMQ (String qManager, String qName, String qHostname, String qChannel, int qPort, String fileDir, String archiveDir, String interfaceName) {

		try {

			File filt = new File(fileDir);
			FileFilter isFile = new IsFileFilter();
			File s[] = filt.listFiles(isFile);

			if (s.length > 0) {
				logger.info("Number of files found in "+fileDir+ " = "+s.length);

				// Set up WebSphere MQ environment
				MQEnvironment.hostname = qHostname;
				MQEnvironment.channel  = qChannel;
				MQEnvironment.port  = qPort;
				
				if(qHostname == null)
				{
					MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,  MQC.TRANSPORT_MQSERIES);
				}
				else
				{
					MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT);
				}

				logger.info("Connecting to queue manager: "+qManager);
				MQQueueManager qMgr = new MQQueueManager(qManager);
				int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT; // Set up the options on the queue we wish to open
				logger.info("Accessing queue: "+qName);
				MQQueue queue = qMgr.accessQueue(qName, openOptions);
				MQMessage ppxfile = new MQMessage(); // Define a simple WebSphere MQ Message ...

				for (int i=0; i < s.length; i++) 
				{
					StringBuffer contents = new StringBuffer();
					BufferedReader reader = null;
					reader = new BufferedReader(new FileReader(s[i]));
					String text = new String();
					contents.append(reader.readLine());
				
					while((text = reader.readLine())!= null)
					{
						contents.append("\n").append(text);
					}//while
					
					contents.substring(0, contents.length()-1);
					ppxfile.format = "MQSTR";
					MQPutMessageOptions pmo = new MQPutMessageOptions();
					logger.info("Sending message "+s[i]+" to "+qManager+"."+qName);
					ppxfile.writeString(contents.toString());
					queue.put(ppxfile, pmo);  // Put the message to the queue
					ppxfile.clearMessage();   //reset message for the next one

					//move file to archive
					reader.close();
					String dateString = getDateString();
					File destDir = new File(archiveDir);
					File destFileName = new File(interfaceName+"."+dateString);
					boolean success = s[i].renameTo(new File(destDir, destFileName.getName()));
					if (!success) {
						logger.severe("Failed to archive: "+s[i]);
						System.exit(-1);
					}
					else {
						logger.info("Successfully archived: "+destDir+"\\"+destFileName);
					}
					try {
						java.lang.Thread.currentThread().sleep(250);
					} catch (Exception e) {
						logger.severe("Caught the following Exception while trying sleep: "+e.getMessage());
						System.exit(-1);
					}
				}//for
			} //if
		} //try
		catch (MQException ex) 
		{
			logger.severe("Caught the following MQException while trying to upload a message to MQ: "
					+ ex.getMessage());
			System.exit(-1);
		}
		catch (IOException ex) 
		{
			logger.severe("Caught the following IOExcepton while trying to upload a message to MQ: "
					+ ex.getMessage());
		}

	} //method uploadToMQ

	public String getDateString() {
		Date date = new Date();
		String dateFormat = "yyyyMMddHHmmssSSS";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}

}