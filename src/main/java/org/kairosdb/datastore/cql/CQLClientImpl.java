package org.kairosdb.datastore.cql;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 Created by bhawkins on 3/4/15.
 */
public class CQLClientImpl implements CQLClient
{
	public static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cql.keyspace";
	private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cql.host_list";


	private final Cluster m_cluster;
	private String m_keyspace;

	@Inject
	public CQLClientImpl(@Named(KEYSPACE_PROPERTY)String keyspace,
						 @Named(HOST_LIST_PROPERTY)String hostList)
	{
		PoolingOptions poolingOptions = new PoolingOptions()
				.setConnectionsPerHost(HostDistance.LOCAL,  2, 4)
				.setConnectionsPerHost(HostDistance.REMOTE,  2, 4)
				.setMaxRequestsPerConnection(HostDistance.LOCAL, 500)
				.setMaxRequestsPerConnection(HostDistance.REMOTE, 500);

		final Cluster.Builder builder = new Cluster.Builder()
				.withPoolingOptions(poolingOptions)
				.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
				.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE));

		for (String node : hostList.split(","))
		{
			builder.addContactPoint(node.split(":")[0]);
		}

		m_cluster = builder.build();
		m_keyspace = keyspace;
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


	public static void main(String[] args) {
		new CQLClientImpl("s", "10.103.17.91,10.103.17.92,10.103.17.93");
	}

}
