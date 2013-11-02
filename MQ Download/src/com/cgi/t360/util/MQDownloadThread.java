package com.cgi.t360.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.ibm.mq.MQC;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;

public class MQDownloadThread extends Thread {
	private Logger logger = null;
	private LogManager lm;
	private Properties prop = new Properties();
	private FileHandler fh;
	
	private MQQueueManager qMgr;
	private MQQueue queue;
	
	MQDownloadThread(Logger log, MQQueueManager qm, MQQueue q)
	{
		logger = log;
		qMgr = qm;
		queue = q;
		
		try 
		{
			prop.load(new FileInputStream("mqdownload.properties"));
			
			//logging
			lm = LogManager.getLogManager();
			fh=new FileHandler(prop.getProperty("LOGNAME"), true);
			lm.addLogger(logger);
			logger.setLevel(Level.INFO);
			fh.setFormatter(new SimpleFormatter());
			logger.addHandler(fh);
		} catch (FileNotFoundException e) {
			logger.severe("Caught FileNotFoundException: "+e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.severe("Caught IOException: "+e.getMessage());
			e.printStackTrace();
		}		
	}
	
	@SuppressWarnings("unchecked")
	public void run()
	{
		try 
		{
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

			qMgr = new MQQueueManager(qManager);

			// Set up the options on the queue we wish to open
			int openOptions = MQC.MQOO_INPUT_AS_Q_DEF;

			// Now specify the queue that we wish to open and the open options
			//System.out.println("Accessing queue: "+qName);
			logger.info("Accessing queue: "+qName);
			queue = qMgr.accessQueue(qName, openOptions);

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
			logger.log(Level.SEVERE, "A WebSphere MQ Error occured : Completion Code "
					+ ex.completionCode + " Reason Code " + ex.reasonCode);
			ex.printStackTrace();
		}
		catch (java.io.IOException ex) {
			logger.log(Level.SEVERE, "An IOException occured whilst writing to the message buffer: "
					+ ex);
			ex.printStackTrace();
		}
		finally 
		{
			try 
			{
				logger.info("Closing MQ objects.");
				queue.close();
				qMgr.close();
				logger.info("Exitting.  Have a nice day.");
			} catch (MQException ex) {
				logger.log(Level.SEVERE, "A WebSphere MQ Error occurred : Completion Code " + ex.completionCode + "Reason Code " + ex.reasonCode);
				ex.printStackTrace();
			}
		}
	}
	
}
