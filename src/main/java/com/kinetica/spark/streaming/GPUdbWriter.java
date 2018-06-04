package com.kinetica.spark.streaming;

import com.gpudb.GPUdb;
import com.gpudb.RecordObject;
import com.gpudb.protocol.InsertRecordsRequest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GPUdb data processor, used in accepting object records or parseable string
 * records of a given type and inserting them into the database
 * 
 * @param <T> type of RecordObject to insert into GPUdb
 */
public class GPUdbWriter<T extends RecordObject> implements Serializable
{
	private static final long serialVersionUID = -5273795273398765842L;

	private static final Logger log = LoggerFactory.getLogger(GPUdbWriter.class);

	/** SparkConf property name for GPUdb connection host */
	public static final String PROP_GPUDB_HOST = "gpudb.host";
	/** SparkConf property name for GPUdb connection port */
	public static final String PROP_GPUDB_PORT = "gpudb.port";
	/** SparkConf property name for GPUdb threads */
	public static final String PROP_GPUDB_THREADS = "gpudb.threads";
	/** SparkConf property name for GPUdb table name */
	public static final String PROP_GPUDB_TABLE_NAME = "gpudb.table";
	/** SparkConf property name for GPUdb insert size */
	public static final String PROP_GPUDB_INSERT_SIZE = "gpudb.insert.size";

	private static final int DEFAULT_PORT = 9191;
	private static final int DEFAULT_THREADS = 4;
	private static final int DEFAULT_INSERT_SIZE = 1;

	private String host;
	private int port;
	private int threads;
	private String tableName;
	private int insertSize;

	private List<T> records = new ArrayList<T>();


	/**
	 * Creates a GPUdbWriter with parameters specified through a
	 * SparkConf.  The required parameters are:
	 * <ul>
	 * <li>gpudb.host - hostname/IP of GPUdb server</li>
	 * <li>gpudb.port - port on which GPUdb service is listening</li>
	 * <li>gpudb.threads - number of threads to use in the writing</li>
	 * <li>gpudb.table - name of table to which records will be written</li>
	 * <li>gpudb.insert.size - number of records to write per write</li>
	 * </ul>
	 * 
	 * @param conf Spark configuration containing the GPUdb setup parameters for
	 *        this reader
	 */
	public GPUdbWriter(SparkConf conf)
	{
		host = conf.get(PROP_GPUDB_HOST);
		port = conf.getInt(PROP_GPUDB_PORT, DEFAULT_PORT);
		threads = conf.getInt(PROP_GPUDB_THREADS, DEFAULT_THREADS);
		tableName = conf.get(PROP_GPUDB_TABLE_NAME);
		insertSize = conf.getInt(PROP_GPUDB_INSERT_SIZE, DEFAULT_INSERT_SIZE);

		if (host == null || host.isEmpty())
			throw new IllegalArgumentException("No GPUdb hostname defined");
		
		if (tableName == null || tableName.isEmpty())
			throw new IllegalArgumentException("No GPUdb table name defined");
	}

	/**
	 * Writes the contents of an RDD to GPUdb
	 * 
	 * @param rdd RDD to write to GPUdb
	 */
	public void write(JavaRDD<T> rdd)
	{
		rdd.foreachPartition
		(
			new VoidFunction<Iterator<T>>()
			{
				private static final long serialVersionUID = 1519062387719363984L;
				
				@Override
				public void call(Iterator<T> tSet)
				{
					while (tSet.hasNext())
					{
						T t = tSet.next();
						if (t != null)
							write(t);
					}
					flush();
				}
			}
		);
	}

	/**
	 * Writes the contents of a Spark stream to GPUdb
	 * 
	 * @param dstream data stream to write to GPUdb
	 */
	public void write(JavaDStream<T> dstream)
	{
		dstream.foreachRDD
		(
			new VoidFunction<JavaRDD<T>>()
			{
				private static final long serialVersionUID = -6215198148637505774L;

				@Override
				public void call(JavaRDD<T> rdd) throws Exception
				{
					rdd.foreachPartition
					(
						new VoidFunction<Iterator<T>>()
						{
							private static final long serialVersionUID = 1519062387719363984L;

							@Override
							public void call(Iterator<T> tSet)
							{
								while (tSet.hasNext())
								{
									T t = tSet.next();
									if (t != null)
										write(t);
								}
							}
						}
					);
				}
			}
		);
	}

	/**
	 * Writes a record to GPUdb
	 *
	 * @param t record to write to GPUdb
	 */
	public void write(T t)
	{
		records.add(t);
		log.debug("Added <{}> to write queue", t);

		if (records.size() >= insertSize)
			flush();
	}

	/**
	 * Flushes the set of accumulated records, writing them to GPUdb
	 */
	public void flush()
	{
		try
		{
			log.debug("Creating new GPUdb...");
			GPUdb gpudb = new GPUdb("http://" + host + ":" + port, new GPUdb.Options().setThreadCount(threads));
	
			List<T> recordsToInsert = records;
			records = new ArrayList<T>();
	
			log.info("Writing <{}> records to table <{}>", recordsToInsert.size(), tableName);
			for (T record : recordsToInsert)
				log.debug("    Array Item: <{}>", record);
	
			InsertRecordsRequest<T> insertRequest = new InsertRecordsRequest<T>();
			insertRequest.setData(recordsToInsert);
			insertRequest.setTableName(tableName);
			gpudb.insertRecords(insertRequest);
		}
		catch (Exception ex)
		{
			log.error("Problem writing record(s)", ex);
		}
	}
}
