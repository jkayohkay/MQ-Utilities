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
			int openOptions = MQC.MQOO_INPUT_AS_Q_DEF| MQC.MQOO_INQUIRE ;			
			
			boolean getMessage = true;
			
			// Now specify the queue that we wish to open and the open options
			//System.out.println("Accessing queue: "+qName);
			logger.info("Accessing queue: "+qName);
			queue = qMgr.accessQueue(qName, openOptions);
			
			if(queue.getCurrentDepth() == 0)
			{
				logger.info("Queue "+qName+" is empty.");
				getMessage = false;
			}
			
			while(getMessage)
			{
				queue = qMgr.accessQueue(qName, openOptions);
				
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
					
					String fileName = getFileName(messageString, message);
					
					File outputFile = new File(fileName);
					
					outputFile.createNewFile();
					FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(messageString);
					bw.close();
					fw.close();
				}
				
				if(prop.getProperty("BatchOrContinuous") != null)
				{
					if((prop.getProperty("BatchOrContinuous").equalsIgnoreCase("batch"))
							&& (queue.getCurrentDepth() == 0))
					{
						getMessage = false;
					}
				}
				
				queue.close();
				qMgr.commit();
			}

		}
		catch (MQException ex) {
			logger.log(Level.SEVERE, "A WebSphere MQ Error occured : Completion Code "
					+ ex.completionCode + " Reason Code " + ex.reasonCode, ex);
		}
		catch (java.io.IOException ex) {
			logger.log(Level.SEVERE, "An IOException occured whilst writing to the message buffer: "
					+ ex, ex);
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
				logger.log(Level.SEVERE, "A WebSphere MQ Error occurred : Completion Code " + ex.completionCode + "Reason Code " + ex.reasonCode, ex);
			}
		}
	}

	/**
	 * 
	 * @param messageString
	 * @param message
	 * @return
	 */
	private String getFileName(String messageString, MQMessage message) 
	{		
		StringBuffer fileName = new StringBuffer();
		
		// Start with the path to the output dir/folder
		fileName.append(prop.getProperty("OutputDir"));
		// Add the filesystem path separator if not already there
		if(!fileName.toString().endsWith(File.separator))
		{
			fileName.append(File.separator);
		}
		
		// Add the MessageID
		if( (messageString.indexOf("<MessageID>") > 0)
				&& (messageString.indexOf("</MessageID>") > 0) )
		{
			String value = messageString.substring((messageString.indexOf("<MessageID>")+"<MessageID>".length()),
											(messageString.indexOf("</MessageID>")));
			fileName.append("MsgID_"+value);
		}
		
		// Add the DestinationID
		if( (messageString.indexOf("<DestinationID>") > 0)
				&& (messageString.indexOf("</DestinationID>") > 0) )
		{
			String value = messageString.substring((messageString.indexOf("<DestinationID>")+"<DestinationID>".length()),
											(messageString.indexOf("</DestinationID>")));
			fileName.append(".DestID_"+value);
		}
		
		// Add the SenderID
		if( (messageString.indexOf("<SenderID>") > 0)
				&& (messageString.indexOf("</SenderID>") > 0) )
		{
			String value = messageString.substring((messageString.indexOf("<SenderID>")+"<SenderID>".length()),
											(messageString.indexOf("</SenderID>")));
			fileName.append(".SenderID_"+value);
		}
		
		// Add the MessageType
		if( (messageString.indexOf("<MessageType>") > 0)
				&& (messageString.indexOf("</MessageType>") > 0) )
		{
			String value = messageString.substring((messageString.indexOf("<MessageType>")+"<MessageType>".length()),
											(messageString.indexOf("</MessageType>")));
			fileName.append(".MsgType_"+value);
		}
		
		// Add the current time in milliseconds and message sequence number to make 
		// the filename unique
		if(fileName.toString().endsWith(File.separator))
		{
			fileName.append(System.currentTimeMillis()
					+message.messageSequenceNumber);
		}
		else
		{
			fileName.append("."
					+System.currentTimeMillis()
					+message.messageSequenceNumber);
		}
		
		// Add the file extension
		fileName.append(".xml");
		
		return fileName.toString();
	}
	
}
