package org.kairosdb.datastore.cql;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.inject.Inject;
import com.google.inject.name.Named;


/**
 Created by bhawkins on 10/13/14.
 */
public class CQLConfiguration
{
	public static final String READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cql.read_consistency_level";
	public static final String WRITE_CONSISTENCY_LEVEL = "kairosdb.datastore.cql.write_consistency_level";

	public static final String ROW_KEY_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cql.row_key_cache_size";
	public static final String STRING_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cql.string_cache_size";

	public static final String SINGLE_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cql.single_row_read_size";
	public static final String MULTI_ROW_SIZE_PROPERTY = "kairosdb.datastore.cql.multi_row_size";


	@Inject
	@Named(WRITE_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataWriteLevel = ConsistencyLevel.QUORUM;

	@Inject
	@Named(READ_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataReadLevel = ConsistencyLevel.ONE;


	@Inject
	@Named(ROW_KEY_CACHE_SIZE_PROPERTY)
	private int m_rowKeyCacheSize = 1024;

	@Inject
	@Named(STRING_CACHE_SIZE_PROPERTY)
	private int m_stringCacheSize = 1024;



	@Inject
	@Named(SINGLE_ROW_READ_SIZE_PROPERTY)
	private int m_singleRowReadSize;

	@Inject
	@Named(MULTI_ROW_SIZE_PROPERTY)
	private int m_multiRowSize;


	public CQLConfiguration()
	{
	}

	public CQLConfiguration(int singleRowReadSize,
							int multiRowSize)
	{
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowSize = multiRowSize;
	}

	public ConsistencyLevel getDataWriteLevel()
	{
		return m_dataWriteLevel;
	}

	public ConsistencyLevel getDataReadLevel()
	{
		return m_dataReadLevel;
	}


	public int getRowKeyCacheSize()
	{
		return m_rowKeyCacheSize;
	}

	public int getStringCacheSize()
	{
		return m_stringCacheSize;
	}

	public int getSingleRowReadSize()
	{
		return m_singleRowReadSize;
	}

	public int getMultiRowSize()
	{
		return m_multiRowSize;
	}

}
