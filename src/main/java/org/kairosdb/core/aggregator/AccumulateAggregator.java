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
package org.kairosdb.core.aggregator;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "accumulate", description = "accumulate data points")
public class AccumulateAggregator extends RangeAggregator
{
	private DoubleDataPointFactory m_dataPointFactory;

	@Inject
	public AccumulateAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return DataPoint.GROUP_NUMBER.equals(groupType);
	}

	@Override
	protected RangeSubAggregator getSubAggregator()
	{
		return (new AccumulateDataPointAggregator());
	}

	private class AccumulateDataPointAggregator implements RangeSubAggregator {

		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange) {
            List<DataPoint> results = Lists.newArrayList();
            double sum = 0;
			while (dataPointRange.hasNext()) {
				DataPoint dataPoint = dataPointRange.next();
                sum += dataPoint.getDoubleValue();
                results.add(m_dataPointFactory.createDataPoint(dataPoint.getTimestamp(), sum));
			}

            return results;
		}
	}
}
