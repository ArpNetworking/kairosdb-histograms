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

import com.arpnetworking.kairosdb.DelegatingAggregatorMap;
import com.google.inject.Inject;
import org.kairosdb.core.aggregator.PercentileAggregator;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import javax.inject.Named;

/**
 * Percentile aggregator that delegates to the built-in and the histogram aggregators.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@FeatureComponent(
        name = "percentile",
        description = "Finds the percentile of the data range.")
public final class DelegatingPercentileAggregator extends DelegatingAggregator {
    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    @Inject
    public DelegatingPercentileAggregator(@Named("percentile") final DelegatingAggregatorMap aggregatorMap) {
        super(aggregatorMap);
    }

    public void setPercentile(final double percentile) {
        _percentile = percentile;
    }


    @Override
    protected void setProperties(final RangeAggregator aggregator) {
        super.setProperties(aggregator);
        if (aggregator instanceof HistogramPercentileAggregator) {
            final HistogramPercentileAggregator histogramPercentileAggregator = (HistogramPercentileAggregator) aggregator;
            histogramPercentileAggregator.setPercentile(_percentile);
        } else if (aggregator instanceof PercentileAggregator) {
            final PercentileAggregator percentileAggregator = (PercentileAggregator) aggregator;
            percentileAggregator.setPercentile(_percentile);
        } else {
            try {
                final PropertyDescriptor pd = new PropertyDescriptor("percentile", aggregator.getClass());
                pd.getWriteMethod().invoke(aggregator, _percentile);
            } catch (final IntrospectionException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @javax.validation.constraints.Min(0)
    @javax.validation.constraints.Max(1)
    private double _percentile = -1d;
}
