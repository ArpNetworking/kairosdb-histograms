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
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Aggregator that computes the apdex from a range of histograms.
 * See https://en.wikipedia.org/wiki/Apdex for more details about Apdex
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@FeatureComponent(
        name = "apdex",
        description = "Computes the Apdex score."
)
public class HistogramApdexAggregator extends RangeAggregator{
    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @param dataPointFactory
     */
    @Inject
    public HistogramApdexAggregator(final DoubleDataPointFactory dataPointFactory) {
        _dataPointFactory = dataPointFactory;
    }

    public void setTarget(final double target) {
        _target = target;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramApdexDataPointAggregator();
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return _dataPointFactory.getGroupType();
    }

    @Valid
    @NotNull
    @Min(0)
    @FeatureProperty(
            name = "target",
            label = "Target latency",
            description = "The Apdex target for latency",
            default_value = "1"
    )
    private double _target = -1d;
    private final DoubleDataPointFactory _dataPointFactory;


    private final class HistogramApdexDataPointAggregator implements RangeSubAggregator {

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            long satisfied = 0;
            long acceptable = 0;
            long total = 0;
            final double acceptableThreshold = _target * 4;
            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    for (final Map.Entry<Double, Integer> entry : hist.getMap().entrySet()) {
                        if (entry.getKey() <= _target) {
                            satisfied += entry.getValue();
                        } else if (entry.getKey() <= acceptableThreshold) {
                            acceptable += entry.getValue();
                        }
                        total += entry.getValue();
                    }
                }
            }

            final double apdex = (satisfied + (acceptable / 2d)) / total;
            return Collections.singletonList(_dataPointFactory.createDataPoint(returnTime, apdex));
        }
    }
}
