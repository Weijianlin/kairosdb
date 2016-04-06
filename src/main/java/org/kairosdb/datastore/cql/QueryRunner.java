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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.*;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.MemoryMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.google.common.base.Preconditions.checkNotNull;

public class QueryRunner {
    private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);


    private final String keyspace;
	private final String table;
    private final Session session;
	private final LocalDateTime startTime;
	private final LocalDateTime endTime;
    private final int startYear;
    private final int endYear;
	private final int singleRowReadSize;
	private final int multiRowSize;

	private final QueryCallback queryCallback;
	private final BlockingQueue<DataPointsRowKey> rowKeyQueue;


    private static final ThreadFactory DAEMON_THREAD_FACTORY =
            new ThreadFactoryBuilder().setDaemon(true).build();


    private final ListeningExecutorService executor =
            MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(1, DAEMON_THREAD_FACTORY)
            );

    private final List<ListenableFuture<ResultSet>> listenableFutures;
    private final int concurrentQueries;

    private final MemoryMonitor memoryMonitor;


    private QueryRunner(Builder builder) {
        this.session = builder.session;
        this.keyspace = builder.keyspace;
        this.table = builder.table;
        this.singleRowReadSize = builder.singleRowReadSize;
        this.multiRowSize = builder.multiRowSize;
        this.queryCallback = builder.queryCallback;

        this.rowKeyQueue = new ArrayBlockingQueue<>(builder.rowKeys.size());
        this.rowKeyQueue.addAll(builder.rowKeys);

        this.concurrentQueries = Math.min(multiRowSize, rowKeyQueue.size());
        this.listenableFutures = Lists.newArrayListWithCapacity(concurrentQueries);

        this.startTime = Times.toLocalDateTime(builder.startTime);
        this.endTime = Times.toLocalDateTime(builder.endTime);

        this.startYear = this.startTime.getYear();
        this.endYear = this.endTime.getYear();

        this.memoryMonitor = new MemoryMonitor(1);
    }

    public static Builder builder(){
        return new Builder();
    }


    private Statement createDataPointsQueryStatement(DataPointsRowKey rowKey){
        short year = rowKey.getYear();
        int startSecOfYear = startYear < year ? 0 : Times.getSecondOfYear(startTime);
        int endSecOfYear = endYear > year ? Integer.MAX_VALUE : Times.getSecondOfYear(endTime);

        return QueryBuilder
                .select("sec_of_year", "value")
                .from(keyspace, table)
                .where(eq("metric", rowKey.getMetric()))
                    .and(eq("tags", rowKey.getTagsString()))
                    .and(eq("year", rowKey.getYear()))
                    .and(gte("sec_of_year", startSecOfYear))
                    .and(lte("sec_of_year", endSecOfYear))
                .orderBy(QueryBuilder.asc("sec_of_year"))
                .disableTracing();
    }

    private ListenableFuture<ResultSet> queryRowKey(DataPointsRowKey rowKey){
        Statement statement = createDataPointsQueryStatement(rowKey)
                .setFetchSize(singleRowReadSize);
        return Futures.transform(
                session.executeAsync(statement),
                iterate(1, rowKey),
                executor);
    }

    private  AsyncFunction<ResultSet, ResultSet> iterate(final int page,
                                                         final DataPointsRowKey rowKey) {
        return rs -> {
            memoryMonitor.checkMemoryAndThrowException();
            QueryRunner.this.writeRows(rowKey, rs.all());

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                DataPointsRowKey nextRowKey = rowKeyQueue.poll();
                if (nextRowKey != null) {
                    return queryRowKey(nextRowKey);
                } else {
                    return Futures.immediateFuture(rs);
                }
            } else {
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return Futures.transform(future, iterate(page + 1, rowKey), executor);
            }
        };
    }

    private void writeRows(DataPointsRowKey rowKey,
                           List<Row> rows)
            throws IOException {
        if (rows == null || rows.isEmpty()){
            return;
        }

        Map<String, String> tags = rowKey.getTags();
        queryCallback.startDataPointSet(LegacyDataPointFactory.DATASTORE_TYPE, tags);
        for (Row r : rows) {
            queryCallback.addDataPoint(
                    new LegacyLongDataPoint(
                            Times.toTimestamp(rowKey.getYear(), r.getInt(0)),
                            r.getLong(1)
                    ));
        }
    }


    public void run() throws DatastoreException {
        for (int i = 0; i < concurrentQueries; i++) {
            DataPointsRowKey rowKey = rowKeyQueue.poll();
            if (rowKey == null){
                break;
            }
            listenableFutures.add(queryRowKey(rowKey));
        }

        for (ListenableFuture<ResultSet> future : listenableFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("query row key error", e);
                throw new DatastoreException(e);
            }
        }
    }


    public static class Builder{
        private String keyspace;
        private String table;
        private Session session;
        private long startTime;
        private long endTime;
        private int singleRowReadSize;
        private int multiRowSize;

        private QueryCallback queryCallback;
        private List<DataPointsRowKey> rowKeys;


        private Builder(){}

        public Builder session(Session session){
            this.session = checkNotNull(session, "session");
            return this;
        }

        public Builder table(String keyspace, String table){
            this.keyspace = checkNotNull(keyspace, "keyspace");
            this.table = checkNotNull(table, "table");
            return this;
        }

        public Builder timeRange(long startTime, long endTime){
            this.startTime = startTime;
            this.endTime = endTime;
            return this;
        }

        public Builder queryCallback(QueryCallback queryCallback){
            this.queryCallback = checkNotNull(queryCallback, "queryCallback");
            return this;
        }

        public Builder rowKeys(List<DataPointsRowKey> rowKeys){
            this.rowKeys = checkNotNull(rowKeys, "rowKeys");
            return this;
        }

        public Builder limit(int singleRowReadSize, int multiRowSize){
            this.singleRowReadSize = singleRowReadSize;
            this.multiRowSize = multiRowSize;
            return this;
        }

        public QueryRunner build(){
            checkNotNull(session, "session");
            checkNotNull(keyspace, "keyspace");
            checkNotNull(table, "table");
            checkNotNull(queryCallback, "queryCallback");
            checkNotNull(rowKeys, "rowKeys");

            return new QueryRunner(this);
        }
    }
}
