package com.cgi.t360.util;

import java.sql.*;
import java.util.*;

/**
 * 
 * @author CGI
 *
 */
public class DBManager {

	private static LogMgr lm = null;
	private static PropertiesManager prop = null;
	private static Connection dbConn = null;
	
	// Database server properties
	private static String dbJDBCType4Driver = null;
	private static String dbVendor = null;
	private static String dbHostname = null;
	private static String dbPortNumber = null;
	private static String dbName = null;
	private static String dbUser = null;
	private static String dbPassword = null;
	
	/**
	 * Constructor for DBManager
	 * 
	 * @param logManager LogMgr instance of log manager to use
	 * @param propertiesManager PropertiesManager instance of properties manager to use
	 */
	public DBManager(LogMgr logManager, PropertiesManager propertiesManager)
	{
		lm = logManager;
		prop = propertiesManager;
		loadDbProperties();
		dbConn = connectToDB();
	}

	/**
	 * Load database properties
	 * 
	 */
	private void loadDbProperties()
	{
		lm.logInfoToFile(this, "Loading database properties...");
		
		String driver = prop.getProperty("dbJDBCType4Driver");
		if(driver == null)
		{
			lm.logErrorToFile(this, "Database JDBC Type 4 driver (dbJDBCType4Driver) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			dbJDBCType4Driver = driver;
			lm.logInfoToFile(this, "dbJDBCType4Driver = "+dbJDBCType4Driver);
		}
		
		String vendor = prop.getProperty("dbVendor");
		if(vendor == null)
		{
			lm.logErrorToFile(this, "Database vendor (dbVendor) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			dbVendor = vendor;
			lm.logInfoToFile(this, "dbVendor = "+dbVendor);
		}

		String hostname = prop.getProperty("dbHostname");
		if(hostname == null)
		{
			lm.logErrorToFile(this, "Database hostname (dbHostname) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			dbHostname = hostname;
			lm.logInfoToFile(this, "dbHostname = "+dbHostname);
		}
		
		String port = prop.getProperty("dbPortNumber");
		if(port == null)
		{
			lm.logErrorToFile(this, "Database port number (dbPortNumber) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			dbPortNumber = port;
			lm.logInfoToFile(this, "dbPortNumber = "+dbPortNumber);
		}
		
		String name = prop.getProperty("dbName");
		if(name == null)
		{
			lm.logErrorToFile(this, "Database name (dbName) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			dbName = name;
			lm.logInfoToFile(this, "dbName = "+dbName);
		}
		
		String user = prop.getProperty("dbUser");
		if(user == null)
		{
			lm.logErrorToFile(this, "Database username (dbUser) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			lm.logInfoToFile(this, "dbUser = "+user);
			dbUser = user;
		}
		
		String password = prop.getProperty("dbPassword");
		if(password == null)
		{
			lm.logErrorToFile(this, "Database password (dbPassword) was not specified in properties file");
			System.exit(-1);
		}
		else
		{
			lm.logInfoToFile(this, "dbPassword = "+password);
			dbPassword = password;
		}
	}
	
	/**
	 * Connect to database using JDBC Type 4 driver
	 * 
	 * @return Connection JDBC connection to database using Type 4 driver
	 */
	private Connection connectToDB()
	{
		Connection conn = null;
		try
		{
			lm.logInfoToFile(this, "Connecting to database...");
			
			// Load JDBC Type 4 Driver
			Class.forName(dbJDBCType4Driver).newInstance();

			// jdbc:oracle:thin:@server.local:1521:prodsid
			// Connect to the database
			conn = DriverManager.getConnection( "jdbc:"
					+dbVendor
					+":@"
					+dbHostname
					+":"
					+dbPortNumber
					+":"
					+dbName,
					dbUser,
					dbPassword);
		}
		catch(SQLException e) 
		{
			lm.logErrorToFile(this, "SQLException caught while trying to connect to database: "+e.getMessage());
			System.exit(-1);
		}
		catch (IllegalAccessException e)
		{
			lm.logErrorToFile(this, "IllegalAccessException caught while trying to connect to database: "+e.getMessage());
			System.exit(-1);
		}
		catch (InstantiationException e)
		{
			lm.logErrorToFile(this, "InstantiationException caught while trying to connect to database: "+e.getMessage());
			System.exit(-1);
		}
		catch (ClassNotFoundException e)
		{
			lm.logErrorToFile(this, "ClassNotFoundException caught while trying to connect to database: "+e.getMessage());
			System.exit(-1);
		}
		
		lm.logInfoToFile(this, "Successfully connected to database");
		return conn;
	}

	public Hashtable<String,String> getListOfMessageTypes(String businessDate)
	{
		String businessDateSql = (businessDate.isEmpty()) ? new String() : new String("and date_business = '"+businessDate+"' ");
		
		String query = new String("select distinct a_intrfc_event_ty from outgoing_intrfc_e where status = 'C' "+businessDateSql 
									+"union "
									+"select distinct a_intrfc_event_ty from outgoing_intrfc_h where status = 'C' "+businessDateSql
									+"union "
									+"select distinct a_intrfc_event_ty from outgoing_telecom_e where status = 'C' "+businessDateSql
									+"union "
									+"select distinct a_intrfc_event_ty from outgoing_telecom_h where status = 'C' "+businessDateSql
									+"order by a_intrfc_event_ty");
		try
		{
			lm.logInfo(this, "Executing the following query: "+query);
			Statement statement = dbConn.createStatement();
			ResultSet rs = statement.executeQuery(query);

			Hashtable<String,String> listOfMessageTypes = new Hashtable<String, String>();
			int counter = 0;
			while(rs.next())
			{
				listOfMessageTypes.put(String.valueOf(rs.getRow()), rs.getString("a_intrfc_event_ty"));
				counter++;
			}
			
			lm.logInfo(this, "Retrieved "+counter+" records.");
			
			rs.close();
			statement.close();
			
			return listOfMessageTypes;
		}
		catch (SQLException e)
		{
			lm.logError(this, "SQLException was caught while trying to get list of message types");
			e.printStackTrace();
			return null;
		}
	}

	public Hashtable<String,String> getXmlUoids(String msgType, String businessDate) 
	{
		String businessDateSql = (businessDate.isEmpty()) ? new String() : new String("and date_business = '"+businessDate+"' ");
		
		String query = new String("select uoid, c_wp_xml_text from outgoing_intrfc_e where status='C' and a_intrfc_event_ty='"+msgType.trim()+"' "+businessDateSql
				+"union "
				+"select uoid, c_wp_xml_text from outgoing_intrfc_h where status='C' and a_intrfc_event_ty='"+msgType.trim()+"' "+businessDateSql
				+"union "
				+"select uoid, c_wp_xml_text from outgoing_telecom_e where status ='C' and a_intrfc_event_ty='"+msgType.trim()+"' "+businessDateSql
				+"union "
				+"select uoid, c_wp_xml_text from outgoing_telecom_h where status ='C' and a_intrfc_event_ty='"+msgType.trim()+"' "+businessDateSql);
		try
		{
			lm.logInfo(this, "Executing the following query: "+query);
			Statement statement = dbConn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			
			Hashtable<String,String> uoids = new Hashtable<String,String>();
			int counter = 0;
			while(rs.next())
			{			
				uoids.put(rs.getString("uoid"), rs.getString("c_wp_xml_text"));
				counter++;
			}
			
			lm.logInfo(this, "Retrieved "+counter+" XML messages for "+msgType);
			
			rs.close();
			statement.close();
			
			return uoids;
		}
		catch (SQLException e)
		{
			lm.logError(this, "SQLException was caught while trying to get list of message types");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	public String getXml(String uoid) {
		if((uoid != null) && (!uoid.isEmpty()))
		{
			String query = new String("select rich_text, rich_text_nclob from wp_outgoing_msgxml where uoid = '"+uoid+"'");
			try
			{
				lm.logInfo(this, "Executing the following query: "+query);
				Statement statement = dbConn.createStatement();
				ResultSet rs = statement.executeQuery(query);

				String xml = null;

				if(rs.next())
				{			
					xml = (rs.getString("rich_text") == null) ? rs.getString("rich_text_nclob") : rs.getString("rich_text");
				}	

				rs.close();
				statement.close();

				return xml;
			}
			catch (SQLException e)
			{
				lm.logError(this, "SQLException was caught while trying to get XML");
				e.printStackTrace();
				System.exit(-1);
			}
		}
		
		return null;
	}
	
}
