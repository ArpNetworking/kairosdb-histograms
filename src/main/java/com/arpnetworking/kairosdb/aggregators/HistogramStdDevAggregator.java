/*
 * Copyright 2018 Bruno Green.
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
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Aggregator that computes the standard deviation value of histograms.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
@FeatureComponent(
        name = "hdev",
        description = "Computes the standard deviation value of the histograms.")
public class HistogramStdDevAggregator extends RangeAggregator {
    private final DoubleDataPointFactory dataPointFactory;

    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @throws KairosDBException on error
     */
    @Inject
    public HistogramStdDevAggregator(final DoubleDataPointFactory dataPointFactory) throws KairosDBException {
        this.dataPointFactory = dataPointFactory;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramStdDevDataPointAggregator();
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return dataPointFactory.getGroupType();
    }

    private final class HistogramStdDevDataPointAggregator implements RangeSubAggregator {

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            long count = 0;
            double mean = 0;
            double m2 = 0;
            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    final NavigableMap<Double, Integer> map = hist.getMap();
                    if (map != null) {
                        for (Map.Entry<Double, Integer> entry : map.entrySet()) {
                            final int n = entry.getValue();
                            if (n > 0) {
                                final double x = entry.getKey();
                                count += n;
                                final double delta = x - mean;
                                mean += ((double) n / count) * delta;
                                m2 += n * delta * (x - mean);
                            }
                        }
                    }
                }
            }

            return Collections.singletonList(dataPointFactory.createDataPoint(returnTime, Math.sqrt(m2 / (count - 1))));
        }
    }
}

