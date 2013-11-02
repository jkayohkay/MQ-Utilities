package com.cgi.t360.util;

import java.util.logging.*;

import com.ibm.mq.*; //Include the WebSphere MQ classes for Java package

public class MQDownload implements Runnable
{

	private static Logger logger = Logger.getLogger(MQDownload.class.getName());
	private static MQQueueManager qMgr;
	private static MQQueue queue;

	// So that we can do some clean-up when the Ctrl-C is sent.
	public void attachShutDownHook(final Thread thread){
		Runtime.getRuntime().addShutdownHook(new Thread() 
		{
			@Override
			public void run() {
				thread.interrupt();
				
				if(queue != null)
				{
					try {
						logger.info("Commiting and closing Queue Manager");
						qMgr.commit();
						qMgr.close();
					} catch (MQException e) {
						logger.severe("Caught MQException when trying to close Queue Manager: "+e.getMessage());
						e.printStackTrace();
					}
				}
			}
		});
	}

	public static void main(String args[]) {
		Thread downloadThread = new Thread(new MQDownloadThread(logger, qMgr, queue));
		downloadThread.start();
		
		MQDownload mqDownload = new MQDownload();
		mqDownload.attachShutDownHook(downloadThread);
	}

	public void run() 
	{
		// Do nothing
	}


}