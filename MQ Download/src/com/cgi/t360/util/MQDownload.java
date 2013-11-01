package com.cgi.t360.util;

import java.io.*;
import java.util.Properties;
import java.util.logging.*;

import com.ibm.mq.*; //Include the WebSphere MQ classes for Java package

public class MQDownload {

	private static final Logger logger = Logger.getLogger(MQDownload.class.getName());
	private static FileHandler fh;
	private static LogManager lm;
	private static Properties prop=new Properties();

	public static void main(String args[]) {
		new MQDownload();
	}

	@SuppressWarnings("unchecked")
	public MQDownload() {

		try {
			prop.load(new FileInputStream("mqdownload.properties"));

			//logging
			lm = LogManager.getLogManager();
			fh=new FileHandler(prop.getProperty("LOGNAME"), true);
			lm.addLogger(logger);
			logger.setLevel(Level.INFO);
			fh.setFormatter(new SimpleFormatter());
			logger.addHandler(fh);

			// define the name of the QueueManager
			String qManager = prop.getProperty("QMANAGER");
			// define the name of the Queue
			String qName = prop.getProperty("QNAME");
			// define the name of your host to connect to
			// Set property QCLIENTMODE to Y if connecting remotely

			if(prop.getProperty("QCLIENTMODE").equals("Y")) 
			{
				logger.info("Setting MQ connection as CLIENT MODE");
				MQEnvironment.properties.put (MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT);
				MQEnvironment.hostname = prop.getProperty("QHOSTNAME");;
				MQEnvironment.channel  = prop.getProperty("QCHANNEL");
				MQEnvironment.port  = Integer.parseInt(prop.getProperty("QPORT"));
			}
			else
			{
				MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,  MQC.TRANSPORT_MQSERIES);
			}

			MQQueueManager qMgr = new MQQueueManager(qManager);

			// Set up the options on the queue we wish to open
			int openOptions = MQC.MQOO_INPUT_AS_Q_DEF;

			// Now specify the queue that we wish to open and the open options
			//System.out.println("Accessing queue: "+qName);
			logger.info("Accessing queue: "+qName);
			MQQueue queue = qMgr.accessQueue(qName, openOptions);

			while(true)
			{
				MQMessage message = new MQMessage();
				MQGetMessageOptions gmo = new MQGetMessageOptions();
				gmo.options = MQC.MQGMO_WAIT;
				gmo.waitInterval = MQC.MQWI_UNLIMITED;
				logger.info("Getting message");
				queue.get(message, gmo);

				if(message.getMessageLength() > 0)
				{
					String messageString = message.readStringOfByteLength(message.getMessageLength());
					logger.info("About to write the message: "+messageString);
					File outputFile = new File(prop.getProperty("OutputDir")+System.currentTimeMillis()+message.messageSequenceNumber+".xml");
					outputFile.createNewFile();
					FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(messageString);
					bw.close();
					fw.close();
				}
			}

		}
		catch (MQException ex) {
			System.out.println("A WebSphere MQ Error occured : Completion Code "
					+ ex.completionCode + " Reason Code " + ex.reasonCode);
			ex.printStackTrace();
		}
		catch (java.io.IOException ex) {
			System.out.println("An IOException occured whilst writing to the message buffer: "
					+ ex);
			ex.printStackTrace();
		}
	}

}