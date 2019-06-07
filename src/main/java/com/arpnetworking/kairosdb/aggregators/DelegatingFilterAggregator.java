package com.arpnetworking.kairosdb.aggregators;

import com.arpnetworking.kairosdb.DelegatingAggregatorMap;
import com.google.inject.Inject;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.plugin.Aggregator;

import javax.inject.Named;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

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

    public void setFilterOp(FilterAggregator.FilterOperation filterop) {
        _filterop = filterop;
    }

    public void setThreshold(double threshold) {
        _threshold = threshold;
    }

    public void setFilterIndeterminateInclusion(HistogramFilterAggregator.FilterIndeterminate inclusion) { _filterinc = inclusion; }

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

        } else {
            try {
                final PropertyDescriptor pd = new PropertyDescriptor("filter_op", aggregator.getClass());
                pd.getWriteMethod().invoke(aggregator, _filterop);
            } catch (final IntrospectionException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
