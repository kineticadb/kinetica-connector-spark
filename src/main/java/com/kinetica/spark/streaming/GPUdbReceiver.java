package com.kinetica.spark.streaming;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.CreateTableMonitorResponse;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;


/**
 * An extension of the Spark Receiver, which establishes a monitor on a GPUdb
 * table and streams new records from it
 * 
 * @author dkatz
 */
public class GPUdbReceiver extends Receiver<AvroWrapper>
{
	private static final long serialVersionUID = -5558211843505774265L;

	private static final Logger log = LoggerFactory.getLogger(GPUdbReceiver.class);

	private GPUdb   gpudb;
	private String  queryURL;
	private String  zmqURL;
	private String  topicID;
	private String  tableName;
	private Type	objectType;  

	/**
	 * Creates a streaming data receiver for the given GPUdb table, via the
	 * given GPUdb service end points
	 * 
	 * @param queryURL base URL for service end point of GPUdb instance hosting
	 *        table to be streamed
	 * @param streamURL base URL for service end point of ZMQ instance providing
	 *        queue for streamed data
	 * @param tableName name of table to monitor for new data
	 */
	public GPUdbReceiver(String queryURL, String streamURL, String tableName)
	{
		super(StorageLevel.MEMORY_AND_DISK_2());
		this.queryURL  = queryURL;
		this.zmqURL	= streamURL;
		this.tableName = tableName;
	}

	@Override
	public void onStart()
	{
		// Start the thread that receives data over a connection
		new Thread()
		{
			@Override
			public void run()
			{
				receive();
			}
		}.start();
	}

	@Override
	public void onStop()
	{
		try
		{
			gpudb.clearTableMonitor(topicID, null);
		}
		catch (GPUdbException ex)
		{
			log.warn("Problem clearing GPUdb table monitor", ex);
		}
	}

	/**
	 * Create a socket connection and receive data until receiver is stopped
	 */
	private void receive()
	{
		try
		{
			// Create table monitor
			gpudb = new GPUdb(queryURL);
			CreateTableMonitorResponse response = gpudb.createTableMonitor(tableName, null);
			topicID = response.getTopicId();
			objectType = Type.fromTable(gpudb, tableName);

			// Attach to table monitor streaming data queue
			@SuppressWarnings("resource")
			ZMQ.Context zmqContext = ZMQ.context(1); 
			@SuppressWarnings("resource")
			Socket subscriber = zmqContext.socket(ZMQ.SUB);
			subscriber.connect(zmqURL);
			subscriber.subscribe(topicID.getBytes());
			subscriber.setReceiveTimeOut(1000);	  
			  
			while (!isStopped())
			{
				ZMsg message = ZMsg.recvMsg(subscriber);
				if (message == null)
					continue;

				boolean skip = true;
	
				for (ZFrame frame : message)
				{
					// Skip first ZFrame
					if (skip)
					{
						skip = false;
						continue;
					}

					if ((frame != null) && (frame.getData() != null))
					{		 
						String schemaStr = objectType.getSchema().toString();
						store(new AvroWrapper(schemaStr, frame.getData()));
					}						
				}				
			}

			restart("Trying to connect again");
		}
		catch(Exception ce)
		{
			restart("Could not connect", ce);
		} 
	}
}