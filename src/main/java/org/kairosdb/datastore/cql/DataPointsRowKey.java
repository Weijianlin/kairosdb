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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.StringPool;

import javax.validation.constraints.NotNull;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class DataPointsRowKey {
    private static final StringPool STRING_POOL = new StringPool();


    private final String metric;
	private final short year;
    private final String tagsString;
	private SortedMap<String, String> tags;


	public DataPointsRowKey(String metric, short year) {
		this(metric, year, "");
	}

	public DataPointsRowKey(String metric, short year,
			SortedMap<String, String> tags) {
		this.metric = checkNotNullOrEmpty(metric);
		this.year = year;
		this.tags = checkNotNull(tags, "tags");
        this.tagsString = generateTagString(tags);
	}

    public DataPointsRowKey(String metric, int year,
                            SortedMap<String, String> tags) throws DatastoreException {
        this(metric, (short)year, tags);
        if (year > Short.MAX_VALUE){
            throw new DatastoreException("data point's timestamp is larger than 32767 year");
        }
    }

    public DataPointsRowKey(String metric, short year, String tagsString) {
        this.metric = checkNotNullOrEmpty(metric);
        this.year = year;
        this.tagsString = checkNotNull(tagsString, "tagsString");
    }

    public DataPointsRowKey(String metric, int year, String tagsString)
            throws DatastoreException {
        this(metric, (short)year, tagsString);
        if (year > Short.MAX_VALUE){
            throw new DatastoreException("data point's timestamp is larger than 32767 year");
        }
    }

    public void addTag(String name, String value) {
		tags.put(name, value);
	}

	public @NotNull String getMetric() {
		return metric;
	}

	public @NotNull SortedMap<String, String> getTags() {
        if (tags == null){
            tags = extractTags(tagsString);
        }
		return tags;
	}

	public short getYear()
	{
		return year;
	}

    public @NotNull String getTagsString(){
        return tagsString;
    }


    @Override
    public String toString() {
        return "DataPointsRowKey{" +
                "metric='" + metric + '\'' +
                ", year=" + year +
                ", tagsString='" + tagsString + '\'' +
                ", tags=" + tags +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataPointsRowKey)) return false;

        DataPointsRowKey that = (DataPointsRowKey) o;

        return year == that.year
                && metric.equals(that.metric)
                && tagsString.equals(that.tagsString);
    }

    @Override
    public int hashCode() {
        int result = metric.hashCode();
        result = 31 * result + year;
        result = 31 * result + tagsString.hashCode();
        return result;
    }

    public static String generateTagString(SortedMap<String, String> tags) {
        if (tags.isEmpty()){
            return "";
        }
        StringBuilder sb = new StringBuilder();
        tags.entrySet().forEach(e -> sb.append(e.getKey()).append('=').append(e.getValue()).append(';'));
        return getString(sb.substring(0, sb.length() - 1));
    }

    public static SortedMap<String, String> extractTags(String tagString){
        SortedMap<String, String> map = Maps.newTreeMap();
        Splitter.on(';').split(tagString).forEach(
                s -> {
                    int idx = s.indexOf('=');
                    map.put(getString(s.substring(0, idx)), getString(s.substring(idx + 1, s.length())));
                }
        );
        return map;
    }


    /**
     If we are pooling strings the string from the pool will be returned.
     @param str string
     @return returns the string or what's in the string pool if using a string pool
     */
    private static String getString(String str) {
        if (STRING_POOL != null)
            return (STRING_POOL.getString(str));
        else
            return (str);
    }

}
