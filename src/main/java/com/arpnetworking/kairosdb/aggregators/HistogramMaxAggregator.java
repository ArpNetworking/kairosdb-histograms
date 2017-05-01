/**
 * Copyright 2017 SmartSheet.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.kairosdb.aggregators;

import com.arpnetworking.kairosdb.HistogramDataPoint;
import com.arpnetworking.kairosdb.HistogramDataPointFactory;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;

import java.util.Collections;
import java.util.Iterator;

/**
 * Aggregator that computes the mean value of histograms.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@AggregatorName(
        name = "hmax",
        description = "Returns the maximum value data point for the time range.",
        properties = {
                @AggregatorProperty(name = "sampling", type = "duration"),
                @AggregatorProperty(name = "align_start_time", type = "boolean")
        }
)
public final class HistogramMaxAggregator extends RangeAggregator {
    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @throws KairosDBException on error
     */
    @Inject
    public HistogramMaxAggregator(final DoubleDataPointFactory dataPointFactory) throws KairosDBException {
        _dataPointFactory = dataPointFactory;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramMeanDataPointAggregator();
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return _dataPointFactory.getGroupType();
    }

    private final DoubleDataPointFactory _dataPointFactory;

    private final class HistogramMeanDataPointAggregator implements RangeSubAggregator {

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            double max = -Double.MAX_VALUE;
            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    max = Math.max(hist.getMax(), max);
                }
            }

            return Collections.singletonList(_dataPointFactory.createDataPoint(returnTime, max));
        }
    }
}
