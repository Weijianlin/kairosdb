package org.kairosdb.datastore.cql;

import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryPlugin;

import java.util.Iterator;

/**
 Created by bhawkins on 11/23/14.
 */
public interface CQLRowKeyPlugin extends QueryPlugin
{
	/**
	 Must return the row keys for a query grouped by time
	 @param query
	 @return
	 */
	public Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query);
}
