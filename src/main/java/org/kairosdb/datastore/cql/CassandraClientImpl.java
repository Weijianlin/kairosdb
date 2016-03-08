package org.kairosdb.datastore.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 Created by bhawkins on 3/4/15.
 */
public class CassandraClientImpl implements CassandraClient
{
	public static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cassandra.keyspace";
	private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.host_list";


	private final Cluster m_cluster;
	private String m_keyspace;

	@Inject
	public CassandraClientImpl(@Named(KEYSPACE_PROPERTY)String keyspace,
			@Named(HOST_LIST_PROPERTY)String hostList)
	{
		final Cluster.Builder builder = new Cluster.Builder()
				.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM));

		for (String node : hostList.split(","))
		{
			builder.addContactPoint(node.split(":")[0]);
		}

		m_cluster = builder.build();
		m_keyspace = keyspace;
	}


	@Override
	public Session getKeyspaceSession()
	{
		return m_cluster.connect(m_keyspace);
	}

	@Override
	public Session getSession()
	{
		return m_cluster.connect();
	}

	@Override
	public String getKeyspace()
	{
		return m_keyspace;
	}

	@Override
	public void close()
	{
		m_cluster.close();
	}


}
