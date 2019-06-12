/**
 * Copyright 2019 Dropbox Inc.
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
import org.kairosdb.core.aggregator.AggregatedDataPointGroupWrapper;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.plugin.Aggregator;

/**
 * Aggregator that filters away some bins of a histogram based on an operation and threshold.
 *
 * @author Joey Jackson
 */
@FeatureComponent(
        name = "hpercentremaining",
        description = "Calculates the percent remaining of the original data points in a histogram")
public class HistogramPercentRemainingAggregator implements Aggregator {
    private final DoubleDataPointFactory _dataPointFactory;

    /**
     * Public Constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @throws KairosDBException on error
     */
    @Inject
    public HistogramPercentRemainingAggregator(final DoubleDataPointFactory dataPointFactory) throws KairosDBException {
        _dataPointFactory = dataPointFactory;
    }

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        return new HistogramPercentRemainingDataPointAggregator(dataPointGroup);
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE;
    }

    private class HistogramPercentRemainingDataPointAggregator extends AggregatedDataPointGroupWrapper {
        HistogramPercentRemainingDataPointAggregator(final DataPointGroup innerDataPointGroup) {
            super(innerDataPointGroup);
        }

        public boolean hasNext() {
            return currentDataPoint != null;
        }

        public DataPoint next() {
            if (currentDataPoint instanceof HistogramDataPoint) {
                final HistogramDataPoint dp = (HistogramDataPoint) currentDataPoint;
                final double percent;
                if (dp.getOriginalCount() > 0) {
                    percent = (double) dp.getSampleCount() / dp.getOriginalCount();
                } else {
                    percent = -1; //Should never start with an empty histogram
                }
                moveCurrentDataPoint();
                return _dataPointFactory.createDataPoint(dp.getTimestamp(), percent);
            } else {
                throw new IllegalArgumentException("Cannot compute percent remaining of non histogram data point");
            }
        }

        private void moveCurrentDataPoint() {
            if (hasNextInternal()) {
                currentDataPoint = nextInternal();
            } else {
                currentDataPoint = null;
            }
        }
    }
}

