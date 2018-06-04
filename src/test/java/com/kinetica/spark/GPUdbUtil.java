package com.kinetica.spark;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.gpudb.protocol.CreateTableRequest;

import org.apache.hadoop.conf.Configuration;


/**
 * Provides common functions required for Spark-based ingestion:
 * 
 * <ul>
 * <li>getGPUdb - returns a GPUdb connection based on Spark configuration</li>
 * <li>getLocalIP - returns local interface IP with the given IP prefix</li>
 * <li>createTable - creates a table with the given GPUdb connection or GPUdb
 *     URL, if it doesn't already exist</li>
 * </ul>
 */
public class GPUdbUtil
{
	private static final String CONFIG_GPUDB_HOST = "gpudb.host";
	private static final String CONFIG_GPUDB_PORT = "gpudb.port";

	/**
	 * Creates a GPUdb connection, using the host and port specified within the
	 * given Hadoop configuration.  The configuration must contain the following
	 * properties:
	 * <ul>
	 * <li>gpudb.host</li>
	 * <li>gpudb.port</li>
	 * </ul>
	 * 
	 * @param config Hadoop configuration, containing connection properties
	 * @return GPUdb connection created
	 * @throws GPUdbException if an error occurs creating the GPUdb connection
	 */
	public static GPUdb getGPUdb(Configuration config) throws GPUdbException
	{
		String gpudbUrl = "http://" + config.get(CONFIG_GPUDB_HOST) + ":" + config.get(CONFIG_GPUDB_PORT);
		GPUdb gpudb = new GPUdb(gpudbUrl);
		return gpudb;
	}

	/**
	 * Searches the current host's interfaces for the first one with an IP
	 * address matching the given prefix
	 * 
	 * @param localHostIpPrefix prefix used to match against local interface IPs
	 * @return the first local interface IP matching the given prefix; null, if
	 *         no match is found
	 * @throws SocketException if an error occurs searching local interfaces
	 */
	public static String getLocalIP(String localHostIpPrefix) throws SocketException
	{
		String localHostIPAddress = null;
	
		Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
		while(e.hasMoreElements())
		{
			NetworkInterface n = e.nextElement();
			Enumeration<InetAddress> ee = n.getInetAddresses();
			while (ee.hasMoreElements())
			{
				InetAddress i = ee.nextElement();
				if (i.getHostAddress().startsWith(localHostIpPrefix))
					localHostIPAddress = i.getHostAddress();
			}
		}
		
		return localHostIPAddress;
	}

	/**
	 * Creates a table within GPUdb
	 * 
	 * @param gpudbUrl HTTP address of GPUdb server in which table should be
	 *        created
	 * @param collectionName name of collection in which to create table
	 * @param tableName name of table to create
	 * @param tableType class name of table type to create
	 * @return true, if table was created successfully;
	 *         false, if not because table already exists
	 * @throws GPUdbException if table didn't exist and failed to be created
	 */
	public static boolean createTable(String gpudbUrl, String collectionName, String tableName, Class<? extends RecordObject> tableType) throws GPUdbException
	{
		GPUdb gpudb = new GPUdb(gpudbUrl);
		
		return createTable(gpudb, collectionName, tableName, tableType);
	}

	/**
	 * Creates a table within GPUdb
	 * 
	 * @param gpudb GPUdb connection to use to create table
	 * @param collectionName name of collection in which to create table
	 * @param tableName name of table to create
	 * @param tableType class name of table type to create
	 * @return true, if table was created successfully;
	 *         false, if not because table already exists
	 * @throws GPUdbException if table didn't exist and failed to be created
	 */
	public static boolean createTable(GPUdb gpudb, String collectionName, String tableName, Class<? extends RecordObject> tableType) throws GPUdbException
	{
		boolean alreadyExists = false;

    	try
		{
		    String typeId = RecordObject.createType(tableType, gpudb);
		    gpudb.addKnownType(typeId, tableType);
		    gpudb.createTable(tableName, typeId, GPUdb.options(CreateTableRequest.Options.COLLECTION_NAME, collectionName));
		}
		catch (GPUdbException e)
		{
			if (e.getMessage().contains("already exists"))
				alreadyExists = true;
			else
			    throw e;
		}
    	
    	return !alreadyExists;
	}
}
