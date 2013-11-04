package ppxmqupload;
import java.io.*;
import java.util.Properties;
import java.util.logging.*;


import com.ibm.mq.*; //Include the WebSphere MQ classes for Java package

public class MQupload {

    private static final Logger logger = Logger.getLogger(MQupload.class.getName());
    private static FileHandler fh;
	private static LogManager lm;
	private static Properties prop=new Properties();

    public static void main(String args[]) {
        new MQupload();
    }

    public MQupload() {

        try {
			prop.load(new FileInputStream("ppxmqupload.properties"));

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
    	    String hostname = new String();
    	    if(prop.getProperty("QCLIENTMODE").equals("Y")) {
            	System.out.println("Setting MQ connection as CLIENT MODE");
        	    hostname = prop.getProperty("QHOSTNAME");
    	    }
    	    int port = Integer.parseInt(prop.getProperty("QPORT"));

    	    // define name of channel for client to use
    	    String channel  = prop.getProperty("QCHANNEL");
    	    // Set up WebSphere MQ environment
    	    MQEnvironment.hostname = hostname;           // Could have put the
    	                                                 // hostname & channel
    	    MQEnvironment.channel  = channel;            // string directly here!
    	    MQEnvironment.port  = port;                  // MQ server port number
    	    //Set TCP/IP or server Connection
    	    MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,  MQC.TRANSPORT_MQSERIES);
    	    //MQEnvironment.properties.put (MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT);

    	    // define the PPX XML directory
    	    String xmldir=prop.getProperty("XMLDIR");
    	    File filt = new File(xmldir);
    		FilenameFilter ext = new XMLFileFilter("XML");
            String s[] = filt.list(ext);
			//System.out.println("Number of XML files found in directory="+s.length);
        	//logger.info("Number of XML files found in directory="+s.length);

        	System.out.println("You are about to upload "+s.length+" XML file(s).");
        	System.out.println("Type Y or y then press <return> to proceed OR press <return> to quit.");
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		    String inString = null;
		    try {
			   inString = in.readLine();
			} catch (IOException e) {
			   // TODO Auto-generated catch block
			   e.printStackTrace();
			}
		    //System.out.println("Echo: " + inString);

        	if (inString.equalsIgnoreCase("y"))  {

               String version="1.1.2";
               logger.info("Running PPXMQUpload version "+version);
               logger.info("Number of XML files found in directory="+s.length);
        	   // Create a connection to the QueueManager
               //System.out.println("Connecting to queue manager: "+qManager);
               logger.info("Connecting to queue manager: "+qManager);
               MQQueueManager qMgr = new MQQueueManager(qManager);

               // Set up the options on the queue we wish to open
               int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_OUTPUT;

               // Now specify the queue that we wish to open and the open options
               //System.out.println("Accessing queue: "+qName);
               logger.info("Accessing queue: "+qName);
               MQQueue queue = qMgr.accessQueue(qName, openOptions);

               // Define a simple WebSphere MQ Message ...
               MQMessage ppxfile = new MQMessage();
               //Load the String array with the names of all the XML files in the directory

               // For each record, read in XML contents, write to String then output to queue
        	   for (int i=0; i < s.length; i++) {
        	      //System.out.println("Processing message="+s[i]);
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

        		  //System.out.println("Sending message "+s[i]);
        	      logger.info("Sending message "+s[i]);
        		  ppxfile.writeString(contents.toString());
        		  queue.put(ppxfile, pmo);  // Put the message to the queue
        		  ppxfile.clearMessage();   //reset message for the next one
        	   }//for

               // Close the queue
               //System.out.println("Closing the queue");
               logger.info("Closing the queue");
               queue.close();

               // Disconnect from the QueueManager
               //System.out.println("Disconnecting from the Queue Manager");
               logger.info("Disconnecting from the Queue Manager");
               qMgr.disconnect();
               System.out.println("Done!");

		    }//if
        }
        catch (MQException ex) {
            System.out.println("A WebSphere MQ Error occured : Completion Code "
                    + ex.completionCode + " Reason Code " + ex.reasonCode);
        }
        catch (java.io.IOException ex) {
            System.out.println("An IOException occured whilst writing to the message buffer: "
                    + ex);
        }
    }//MQupload

}