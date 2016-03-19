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

import com.google.common.collect.Maps;
import org.kairosdb.util.StringPool;

import javax.validation.constraints.NotNull;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class DataPointsRowKey {
    private static final StringPool STRING_POOL = new StringPool();


    private final String metric;
	private final int year;
    private final String tagsString;
	private SortedMap<String, String> tags;


	public DataPointsRowKey(String metric, int year) {
		this(metric, year, "");
	}

	public DataPointsRowKey(String metric, int year,
			SortedMap<String, String> tags) {
		this.metric = checkNotNullOrEmpty(metric);
		this.year = year;
		this.tags = checkNotNull(tags, "tags");
        this.tagsString = generateTagString(tags);
	}

    public DataPointsRowKey(String metric, int year, String tagsString) {
        this.metric = checkNotNullOrEmpty(metric);
        this.year = year;
        this.tagsString = checkNotNull(tagsString, "tagsString");
    }

	public void addTag(String name, String value)
	{
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

	public int getYear()
	{
		return year;
	}

    public @NotNull String getTagsString(){
        return tagsString;
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
        StringBuilder sb = new StringBuilder();
        tags.entrySet().forEach(e -> sb.append(e.getKey()).append('=').append(e.getValue()).append(';'));
        return getString(sb.substring(0, sb.length() - 1));
    }

    public static void extractTags(DataPointsRowKey rowKey, String tagString) {
        int mark = 0;
        int position = 0;
        String tag = null;
        String value;

        for (position = 0; position < tagString.length(); position ++) {
            if (tag == null) {
                if (tagString.charAt(position) == '=') {
                    tag = tagString.substring(mark, position);
                    mark = position +1;
                }
            }
            else {
                if (tagString.charAt(position) == ':'){
                    value = tagString.substring(mark, position);
                    mark = position +1;

                    rowKey.addTag(getString(tag), getString(value));
                    tag = null;
                }
            }
        }
    }

    public static SortedMap<String, String> extractTags(String tagString){
        SortedMap<String, String> map = Maps.newTreeMap();
        int mark = 0;
        int position = 0;
        String tag = null;
        String value;

        for (position = 0; position < tagString.length(); position ++) {
            if (tag == null) {
                if (tagString.charAt(position) == '=') {
                    tag = tagString.substring(mark, position);
                    mark = position +1;
                }
            }
            else {
                if (tagString.charAt(position) == ':'){
                    value = tagString.substring(mark, position);
                    mark = position +1;

                    map.put(getString(tag), getString(value));
                    tag = null;
                }
            }
        }
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
