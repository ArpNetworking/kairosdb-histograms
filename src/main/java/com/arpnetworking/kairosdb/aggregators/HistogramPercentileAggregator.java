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
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.http.rest.validation.NonZero;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Aggregator that computes a percentile of histograms.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@FeatureComponent(
        name = "hpercentile",
        description = "Finds the percentile of the data range.")
public final class HistogramPercentileAggregator extends RangeAggregator {
    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @throws KairosDBException on error
     */
    @Inject
    public HistogramPercentileAggregator(final DoubleDataPointFactory dataPointFactory) throws KairosDBException {
        _dataPointFactory = dataPointFactory;
    }

    public void setPercentile(final double percentile) {
        _percentile = percentile;
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

    @NonZero
    @FeatureProperty(
            label = "Percentile",
            description = "Data points returned will be in this percentile.",
            default_value = "0.1",
            validations =  {
                    @ValidationProperty(
                            expression = "value > 0",
                            message = "Percentile must be greater than 0."
                    ),
                    @ValidationProperty(
                            expression = "value < 1",
                            message = "Percentile must be smaller than 1."
                    )
            }
    )
    private double _percentile = -1d;
    private final DoubleDataPointFactory _dataPointFactory;

    private final class HistogramMeanDataPointAggregator implements RangeSubAggregator {

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            final TreeMap<Double, Integer> merged = Maps.newTreeMap();
            long count = 0;
            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    for (final Map.Entry<Double, Integer> entry : hist.getMap().entrySet()) {
                        count += entry.getValue();
                        merged.compute(entry.getKey(), (key, existing) ->  entry.getValue() + (existing == null ? 0 : existing));
                    }
                }
            }

            final long target = (long) Math.ceil(_percentile * count);
            long current = 0;
            final Iterator<Map.Entry<Double, Integer>> entryIterator = merged.entrySet().iterator();
            while (entryIterator.hasNext()) {
                final Map.Entry<Double, Integer> entry = entryIterator.next();
                current += entry.getValue();
                if (current >= target) {
                    return Collections.singletonList(_dataPointFactory.createDataPoint(returnTime, entry.getKey()));
                }
            }
            return Collections.emptyList();
        }
    }
}
