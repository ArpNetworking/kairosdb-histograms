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

import com.arpnetworking.kairosdb.DelegatingAggregatorMap;
import com.google.inject.Inject;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.plugin.Aggregator;

import javax.inject.Named;

/**
 * Filter aggregator that delegates to the built-in and the histogram aggregators.
 *
 * @author Joey Jackson
 */
@FeatureComponent(
        name = "filter",
        description = "Filters datapoints according to filter operation with a null data point.")
public class DelegatingFilterAggregator extends DelegatingAggregator {
    private FilterAggregator.FilterOperation _filterop;
    private HistogramFilterAggregator.FilterIndeterminate _filterinc;
    private double _threshold;

    /**
     * Setter for filter operation.
     *
     * @param filterop the filter operation
     */
    public void setFilterOp(final FilterAggregator.FilterOperation filterop) {
        _filterop = filterop;
    }

    /**
     * Setter for filter threshold.
     *
     * @param threshold the filter threshold
     */
    public void setThreshold(final double threshold) {
        _threshold = threshold;
    }

    /**
     * Setter for filter inclusion.
     *
     * @param inclusion the filter inclusion
     */
    public void setFilterIndeterminateInclusion(final HistogramFilterAggregator.FilterIndeterminate inclusion) {
        _filterinc = inclusion;
    }

    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    @Inject
    public DelegatingFilterAggregator(@Named("filter") final DelegatingAggregatorMap aggregatorMap) {
        super(aggregatorMap);
    }

    @Override
    protected void setProperties(final Aggregator aggregator) {
        super.setProperties(aggregator);

        if (aggregator instanceof HistogramFilterAggregator) {
            final HistogramFilterAggregator histogramFilterAggregator = (HistogramFilterAggregator) aggregator;
            histogramFilterAggregator.setFilterOp(_filterop);
            histogramFilterAggregator.setThreshold(_threshold);
            histogramFilterAggregator.setFilterIndeterminateInclusion(_filterinc);

        } else if (aggregator instanceof FilterAggregator) {
            final FilterAggregator filterAggregator = (FilterAggregator) aggregator;
            filterAggregator.setFilterOp(_filterop);
            filterAggregator.setThreshold(_threshold);

        }
    }
}
