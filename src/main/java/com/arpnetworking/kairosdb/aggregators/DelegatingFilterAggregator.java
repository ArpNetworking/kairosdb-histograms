package com.arpnetworking.kairosdb.aggregators;

import com.arpnetworking.kairosdb.DelegatingAggregatorMap;
import com.google.inject.Inject;
import org.kairosdb.core.annotation.FeatureComponent;

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
    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    @Inject
    public DelegatingFilterAggregator(@Named("filter") final DelegatingAggregatorMap aggregatorMap) {
        super(aggregatorMap);
    }
}
