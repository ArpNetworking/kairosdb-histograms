/*
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
import com.arpnetworking.kairosdb.HistogramDataPointV2Factory;
import com.arpnetworking.kairosdb.HistogramDataPointV2Impl;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.exception.KairosDBException;

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
        name = "merge",
        description = "Merges histograms.")
public final class HistogramMergeAggregator extends RangeAggregator {
    /**
     * Public constructor.
     *
     * @throws KairosDBException on error
     */
    @Inject
    public HistogramMergeAggregator() throws KairosDBException { }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramMeanDataPointAggregator();
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPointV2Factory.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return HistogramDataPointV2Factory.GROUP_TYPE;
    }

    private static final class HistogramMeanDataPointAggregator implements RangeSubAggregator {
        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            TreeMap<Double, Integer> merged = Maps.newTreeMap();
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sum = 0;
            long count = 0;
            int precision = 64;
            int originalCount = 0;

            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;

                    // If precision is less than our current precision, we need to re-evaluate the values in the map
                    // with the lower precision
                    if (hist.getPrecision() < precision) {
                        final int newPrecision = hist.getPrecision();
                        final TreeMap<Double, Integer> downsampled = Maps.newTreeMap();
                        merged.forEach((k, v) -> downsampled.put(HistogramDataPoint.truncate(k, newPrecision), v));
                        merged = downsampled;
                        precision = newPrecision;

                    }

                    for (final Map.Entry<Double, Integer> entry : hist.getMap().entrySet()) {
                        merged.compute(entry.getKey(), (key, existing) ->  entry.getValue() + (existing == null ? 0 : existing));
                        count += entry.getValue();
                    }

                    originalCount += hist.getOriginalCount();

                    min = Math.min(min, hist.getMin());
                    max = Math.max(max, hist.getMax());
                    sum += hist.getSum();
                }
            }

            final double mean = sum / count;

            return Collections.singletonList(new HistogramDataPointV2Impl(returnTime, precision, merged, min, max, mean, sum, originalCount));
        }
    }
}
