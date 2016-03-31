/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.datastore.cql;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import com.google.common.primitives.Shorts;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.google.common.base.Preconditions.checkNotNull;

public class CQLDatastore implements Datastore {
	public static final Logger logger = LoggerFactory.getLogger(CQLDatastore.class);


	public static final String TABLE_DATA_POINTS = "data_points";
	public static final String TABLE_TAG_INDEX = "tag_index";
	public static final String TABLE_STRING_INDEX = "string_index";

    public static final String ROW_KEY_METRIC_NAMES = "metric_names";
    public static final String ROW_KEY_TAG_NAMES = "tag_names";
    public static final String ROW_KEY_TAG_VALUES = "tag_values";

	public static final String CREATE_KEYSPACE =
			"CREATE KEYSPACE IF NOT EXISTS %s" +
			"  WITH REPLICATION = {'class': 'SimpleStrategy'," +
			"  'replication_factor' : 2}";

	public static final String CREATE_DATA_POINTS_TABLE =
			"CREATE TABLE IF NOT EXISTS %s." +  TABLE_DATA_POINTS + " (\n" +
			"  metric text,\n" +
			"  tags text,\n" +
			"  year smallint,\n" +
			"  sec_of_year int,\n" +
			"  value counter,\n" +
			"  PRIMARY KEY ((metric, year, tags), sec_of_year)\n" +
			")";

	public static final String CREATE_TAG_INDEX_TABLE =
			"CREATE TABLE IF NOT EXISTS %s." + TABLE_TAG_INDEX + " (\n" +
			"  metric text,\n" +
			"  year smallint,\n" +
			"  tags set<text>,\n" +
			"  PRIMARY KEY (metric, year)\n" +
			")";

	public static final String CREATE_STRING_INDEX_TABLE =
			"CREATE TABLE IF NOT EXISTS %s." + TABLE_STRING_INDEX + " (\n" +
			"  name text,\n" +
			"  value text,\n" +
			"  PRIMARY KEY (name, value)\n" +
			") WITH COMPACT STORAGE";

	public static final String INSERT_TAG_INDEX = "INSERT INTO %s." + TABLE_TAG_INDEX +
			" (metric, tags) VALUES (?, ?)";

	public static final String INSERT_STRING_INDEX = "INSERT INTO %s." + TABLE_STRING_INDEX +
			" (name, value) VALUES (?, ?)";


	public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";



    private final CQLClient cqlClient;
	private final String keyspace;

    private final Session session;

	private final PreparedStatement psInsertString;
    private final CQLConfiguration cqlConfiguration;
    //End new props


	private int singleRowReadSize;
	private int multiRowSize;

	private DataCache<DataPointsRowKey> rowKeyCache = new DataCache<>(1024);
	private DataCache<String> metricNameCache = new DataCache<>(1024);
	private DataCache<String> tagNameCache = new DataCache<>(1024);
	private DataCache<String> tagValueCache = new DataCache<>(1024);




	@Inject
	public CQLDatastore(
			CQLClient cqlClient,
			CQLConfiguration cqlConfiguration) throws DatastoreException
	{
        this.cqlClient = cqlClient;


        this.keyspace = cqlClient.getKeyspace();
		this.session = cqlClient.getSession();

		setupSchema();

		//Prepare queries
		psInsertString = session.prepare(formatWithKeyspace(INSERT_STRING_INDEX));

        this.cqlConfiguration = cqlConfiguration;
        singleRowReadSize = cqlConfiguration.getSingleRowReadSize();
        multiRowSize = cqlConfiguration.getMultiRowSize();

        rowKeyCache = new DataCache<>(cqlConfiguration.getRowKeyCacheSize());
        metricNameCache = new DataCache<>(cqlConfiguration.getStringCacheSize());
        tagNameCache = new DataCache<>(cqlConfiguration.getStringCacheSize());
        tagValueCache = new DataCache<>(cqlConfiguration.getStringCacheSize());
    }

    private String formatWithKeyspace(String statment){
        return String.format(statment, keyspace);
    }

    private void setupSchema(){
        session.execute(formatWithKeyspace(CREATE_KEYSPACE));
        session.execute(formatWithKeyspace(CREATE_DATA_POINTS_TABLE));
        session.execute(formatWithKeyspace(CREATE_TAG_INDEX_TABLE));
        session.execute(formatWithKeyspace(CREATE_STRING_INDEX_TABLE));
    }

	private void putInternalDataPoint(String metricName,
                                      ImmutableSortedMap<String, String> tags,
                                      DataPoint dataPoint) {
		try {
			putDataPoint(metricName, tags, dataPoint, 0);
		} catch (DatastoreException e) {
			logger.error("", e);
		}
	}


	public void cleanRowKeyCache() {
        int year = LocalDateTime.now().getYear();
		Set<DataPointsRowKey> keys = rowKeyCache.getCachedKeys();

		for (DataPointsRowKey key : keys) {
			if (key.getYear() != year) {
				rowKeyCache.removeKey(key);
			}
		}
	}


	@Override
	public void close() throws InterruptedException {
		session.close();
		cqlClient.close();
	}

	@Override
	public void putDataPoint(String metricName,
			                 ImmutableSortedMap<String, String> tags,
                             DataPoint dataPoint,
			                 int ttl)
            throws DatastoreException {
		try {
			LocalDateTime dateTime = Times.toLocalDateTime(dataPoint.getTimestamp());
			int year = dateTime.getYear();

            int secondOfYear = Times.getSecondOfYear(dateTime);

            DataPointsRowKey rowKey = new DataPointsRowKey(metricName, year, tags);

			DataPointsRowKey cachedKey = writeMetricTagsIfNew(rowKey);
			if (cachedKey != null) {
                rowKey = cachedKey;
            }

            writeMetricNameIfNew(metricName, dataPoint, rowKey);
            writeTagNameAndValueIfNew(metricName, tags);

            String tagsStr = rowKey.getTagsString();
            incrDataPointValue(metricName, tagsStr, year, secondOfYear, dataPoint.getLongValue());
		} catch (Exception e) {
			throw new DatastoreException(e);
		}
	}



    private @Nullable DataPointsRowKey writeMetricTagsIfNew(DataPointsRowKey rowKey){
        //Write out the row key if it is not cached
        DataPointsRowKey cachedKey = rowKeyCache.cacheItem(rowKey);
        if (cachedKey == null) {
            Statement statement = QueryBuilder
                    .update(keyspace, TABLE_TAG_INDEX)
                    .with(QueryBuilder.add("tags", rowKey.getTagsString()))
                    .where(eq("metric", rowKey.getMetric()))
                        .and(eq("year", rowKey.getYear()))
                    .setConsistencyLevel(ConsistencyLevel.QUORUM);
            session.executeAsync(statement);
        }
        return cachedKey;
    }

    private void writeMetricNameIfNew(String metricName, DataPoint dataPoint, DataPointsRowKey rowKey) {
        //Write metric name if not in cache
        String cachedName = metricNameCache.cacheItem(metricName);
        if (cachedName == null) {
            if (metricName.isEmpty()) {
                logger.warn("Attempted to add empty metric name to string index. Row looks like: {}", dataPoint);
            }
            session.executeAsync(psInsertString.bind(ROW_KEY_METRIC_NAMES, rowKey.getMetric()));
        }
    }

    private void writeTagNameAndValueIfNew(String metricName, ImmutableSortedMap<String, String> tags) {
        //Check tag names and values to write them out
        tags.entrySet().forEach( e -> {
            writeTagNameIfNew(metricName, e.getKey());
            writeTagValueIfNew(metricName, e.getKey(), e.getValue());
        });
    }

    private void writeTagNameIfNew(String metricName, String tagName) {
        String cachedTagName = tagNameCache.cacheItem(tagName);
        if (cachedTagName == null) {
            if(tagName.isEmpty()) {
                logger.warn("Attempted to add empty tagName to string cache for metric: {}", metricName);
            }
            session.executeAsync(psInsertString.bind(ROW_KEY_TAG_NAMES, tagName));
        }
    }

    private void writeTagValueIfNew(String metricName, String tagName, String value) {
        String cachedValue = tagValueCache.cacheItem(value);
        if (cachedValue == null) {
            if(Strings.isNullOrEmpty(value)) {
                logger.warn("Attempted to add empty tagValue (tag name {}) to string cache for metric: {}", tagName, metricName);
            }
            session.executeAsync(psInsertString.bind(ROW_KEY_TAG_VALUES, value));
        }
    }

    private void incrDataPointValue(String metricName,
                                    String tagsStr,
                                    int year,
                                    int secondOfYear,
                                    long value) {
        Statement statement = QueryBuilder
                .update(keyspace, TABLE_DATA_POINTS)
                .where(eq("metric", metricName))
                .and(eq("tags", tagsStr))
                .and(eq("year", year))
                .and(eq("sec_of_year", secondOfYear))
                .with(incr("value", value))
                .setConsistencyLevel(ConsistencyLevel.QUORUM);

        session.executeAsync(statement);
    }


    @Override
	public Iterable<String> getMetricNames() {
        return queryStringIndexTable(ROW_KEY_METRIC_NAMES);
	}

	@Override
	public Iterable<String> getTagNames() {
		return queryStringIndexTable(ROW_KEY_TAG_NAMES);
	}

	@Override
	public Iterable<String> getTagValues(){
		return queryStringIndexTable(ROW_KEY_TAG_VALUES);
	}

    private Iterable<String> queryStringIndexTable(String rowKey){
        Statement statement = QueryBuilder
                .select("value")
                .from(keyspace, TABLE_STRING_INDEX)
                .where(eq("name", rowKey))
                .limit(singleRowReadSize)
                .disableTracing();

        List<String> list = Lists.newArrayList();
        session.execute(statement)
                .forEach(r -> list.add(r.getString(0)));
        return list;
    }

	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query) {
		TagSetImpl tagSet = new TagSetImpl();
		Iterator<DataPointsRowKey> rowKeys = getKeysForQueryIterator(query);

		MemoryMonitor mm = new MemoryMonitor(20);
		while (rowKeys.hasNext()) {
			DataPointsRowKey dataPointsRowKey = rowKeys.next();
			for (Map.Entry<String, String> tag : dataPointsRowKey.getTags().entrySet()) {
				tagSet.addTag(tag.getKey(), tag.getValue());
				mm.checkMemoryAndThrowException();
			}
		}

		return (tagSet);
	}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback)
            throws DatastoreException {
		queryWithRowKeys(query, queryCallback, getKeysForQueryIterator(query));
	}

	private void queryWithRowKeys(DatastoreMetricQuery query,
                                  QueryCallback queryCallback,
                                  Iterator<DataPointsRowKey> rowKeys)
            throws DatastoreException {
        int singleSize = query.getLimit() != 0 ? query.getLimit() : singleRowReadSize;

        try {
            QueryRunner queryRunner = QueryRunner.builder()
                    .session(session)
                    .table(keyspace, TABLE_DATA_POINTS)
                    .rowKeys(Lists.newArrayList(rowKeys))
                    .timeRange(query.getStartTime(), query.getEndTime())
                    .limit(singleSize, multiRowSize)
                    .queryCallback(queryCallback)
                    .build();

            queryRunner.run();

        } catch (Exception e){
            logger.error("query error", e);
            throw new DatastoreException(e);
        }

        try {
            queryCallback.endDataPoints();
        } catch (IOException e) {
            logger.error("query error", e);
        }
	}

	@Override
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery)
            throws DatastoreException {
		checkNotNull(deleteQuery);

		long now = System.currentTimeMillis();

        long startTime = deleteQuery.getStartTime();
        long endTime = deleteQuery.getEndTime();

		boolean deleteAll = (startTime == Long.MIN_VALUE && endTime == Long.MAX_VALUE);

		Iterator<DataPointsRowKey> rowKeyIterator = getKeysForQueryIterator(deleteQuery);
		List<DataPointsRowKey> partialRows = new ArrayList<>();

		while (rowKeyIterator.hasNext()) {
			DataPointsRowKey rowKey = rowKeyIterator.next();
			int rowKeyYear = rowKey.getYear();
            // delete whole row
			if (startTime <= Times.startMilliOfYear(rowKeyYear)
                    && (endTime >= Times.endMilliOfYear(rowKeyYear))) {
                deleteDataPointsRow(rowKey);
                deleteMetricTag(rowKey);
				rowKeyCache.clear();
			} else {
				partialRows.add(rowKey);
			}
		}

		queryWithRowKeys(deleteQuery, new DeletingCallback(deleteQuery.getName()), partialRows.iterator());

		// If index is gone, delete metric name from Strings column family
		if (deleteAll) {
            deleteMetricTagRow(deleteQuery.getName());
            deleteMetricString(deleteQuery.getName());
			rowKeyCache.clear();
			metricNameCache.clear();
		}
	}

    private void deleteMetricString(String metric){
        Statement statement = QueryBuilder
                .delete()
                .from(keyspace, TABLE_STRING_INDEX)
                .where(eq("name", ROW_KEY_METRIC_NAMES))
                    .and(eq("value", metric))
                .disableTracing();
        session.executeAsync(statement);
    }

    private void deleteMetricTagRow(String metric){
        Statement statement = QueryBuilder
                .delete()
                .from(keyspace, TABLE_DATA_POINTS)
                .where(eq("metric", metric))
                .disableTracing();
        session.executeAsync(statement);
    }

    private void deleteMetricTag(DataPointsRowKey rowKey){
        Statement statement = QueryBuilder
                .update(keyspace, TABLE_DATA_POINTS)
                .with(QueryBuilder.remove("tags", rowKey.getTagsString()))
                .where(eq("metric", rowKey.getMetric()))
                    .and(eq("year", rowKey.getYear()))
                .disableTracing();
        session.executeAsync(statement);
    }

    private void deleteDataPointsRow(DataPointsRowKey rowKey){
        Statement statement = QueryBuilder
                .delete()
                .from(keyspace, TABLE_TAG_INDEX)
                .where(eq("metric", rowKey.getMetric()))
                    .and(eq("year", rowKey.getYear()))
                    .and(eq("tags", rowKey.getTagsString()))
                .disableTracing();
        session.executeAsync(statement);
    }



	private SortedMap<String, String> getTags(DataPointRow row) {
		TreeMap<String, String> map = new TreeMap<String, String>();
		for (String name : row.getTagNames()) {
			map.put(name, row.getTagValue(name));
		}

		return map;
	}

	/**
	 * Returns the row keys for the query in tiers ie grouped by row key timestamp
	 *
	 * @param query query
	 * @return row keys for the query
	 */
	public Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query) {
		Iterator<DataPointsRowKey> ret = null;

		List<QueryPlugin> plugins = query.getPlugins();

		//First plugin that works gets it.
		for (QueryPlugin plugin : plugins)
		{
			if (plugin instanceof CQLRowKeyPlugin)
			{
				ret = ((CQLRowKeyPlugin) plugin).getKeysForQueryIterator(query);
				break;
			}
		}

		//Default to old behavior if no plugin was provided
		if (ret == null)
		{
			ret = new FilteredRowKeyIteratorCreator(query.getName(), query.getStartTime(),
					query.getEndTime(), query.getTags()).create();
		}

		return (ret);
	}


	private class FilteredRowKeyIteratorCreator {
        private final List<String> filterTagList;

        private final String metric;
        private final int startYear;
        private final int endYear;

		public FilteredRowKeyIteratorCreator(String metric,
                                             long startTime,
                                             long endTime,
                                             SetMultimap<String, String> filterTags) {
			this.metric = metric;
            if (startTime > endTime) { //swap startTime and endTime
                startTime ^= endTime;
                endTime ^= startTime;
                startTime ^= endTime;
            }
            this.startYear = validYear(Times.toLocalDateTime(startTime).getYear());
            this.endYear = validYear(Times.toLocalDateTime(endTime).getYear());

            this.filterTagList = filterTags.entries().stream()
                    .map(e -> e.getKey() + '=' + e.getValue()).collect(Collectors.toList());

		}

        private int validYear(int year){
            if (year < 0 ){
                return 0;
            }
            if (year > Short.MAX_VALUE){
                return Short.MAX_VALUE;
            }
            return year;
        }

        private Iterator<DataPointsRowKey> create(){
            long startTime = System.currentTimeMillis();

            List<DataPointsRowKey> rowKeys = Lists.newArrayList();
            Statement statement = createMetricTagQueryStatement();
			session.execute(statement).forEach(r -> {
                short year = r.getShort(0);
				r.getSet(1, String.class).stream()
                        .filter(this::tagsWanted)
                        .forEach(tags -> rowKeys.add(new DataPointsRowKey(metric, year, tags)));
			});

            ThreadReporter.addDataPoint(KEY_QUERY_TIME, System.currentTimeMillis() - startTime);
			return rowKeys.iterator();
		}

        private Statement createMetricTagQueryStatement() {
            return QueryBuilder
                .select("year", "tags")
                .from(keyspace, TABLE_TAG_INDEX)
                .where(eq("metric", metric))
                    .and(gte("year", startYear))
                    .and(lte("year", endYear))
                .disableTracing();
        }

        private boolean tagsWanted(String tags) {
			return filterTagList.stream().allMatch(tags::contains);
        }

	}

	private class DeletingCallback implements QueryCallback {
		private String currentTags;
		private final String metric;

		public DeletingCallback(String metric) {
			this.metric = metric;
		}


		@Override
		public void addDataPoint(DataPoint datapoint) throws IOException {
			long time = datapoint.getTimestamp();

            LocalDateTime dateTime = Times.toLocalDateTime(time);
            int year = dateTime.getYear();
            int secondOfYear = Times.getSecondOfYear(dateTime);

            Statement statement = QueryBuilder
                    .delete()
                    .from(keyspace, TABLE_DATA_POINTS)
                    .where(eq("metric", metric))
                        .and(eq("year", year))
                        .and(eq("tags", currentTags))
                        .and(eq("sec_of_year", secondOfYear))
                    .disableTracing();
            session.executeAsync(statement);
		}

		@Override
		public void startDataPointSet(String dataType, Map<String, String> tags)
                throws IOException {
			currentTags = DataPointsRowKey.generateTagString(new TreeMap<>(tags));
		}

		@Override
		public void endDataPoints() {
		}
	}
}
