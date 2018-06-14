/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kinetica.spark;

import com.gpudb.ColumnProperty;
import com.gpudb.RecordObject;
import java.io.Serializable;

/**
 * Simple representation of a person, which extends RecordObject so that it can
 * be written directly to GPUdb
 *
 * @author ksutton
 */
public class PersonRecord extends RecordObject implements Serializable
{
	private static final long serialVersionUID = 3767889270386618741L;

	/** Unique ID of person */
	@RecordObject.Column(order = 0, properties = { ColumnProperty.DATA, ColumnProperty.PRIMARY_KEY })
	public long id;

	/** Name of person */
	@RecordObject.Column(order = 1)
	public String name;

	/** Date of birth of person, in milliseconds since the epoch */
	@RecordObject.Column(order = 2, properties = { ColumnProperty.TIMESTAMP })
	public long birthDate;

	/**
	 * Creates a person with no values specified
	 */
	public PersonRecord() {}

	/**
	 * Creates a person with the specified values
	 *
	 * @param id unique ID of person
	 * @param name name of person
	 * @param birthDate date of birth of person, in milliseconds since the epoch
	 */
	public PersonRecord(long id, String name, long birthDate)
	{
		this.id = id;
		this.name = name;
		this.birthDate = birthDate;
	}
}
