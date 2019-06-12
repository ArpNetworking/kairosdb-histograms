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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.testing.ListDataPointGroup;

import static org.kairosdb.testing.HistogramUtils.createGroup;
import static org.kairosdb.testing.HistogramUtils.createHistogram;

/**
 * Unit tests for the Histogram Percent Remaining Aggregator.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class HistogramPercentRemainingAggregatorTest {

    private HistogramPercentRemainingAggregator _percentRemainingAggregator;
    private HistogramFilterAggregator _filterAggregator;
    private HistogramMergeAggregator _mergeAggregator;
    
//    private static final DataPointGroup SINGLE_HIST_GROUP =
//            createGroup(createHistogram(1L, 1d, 10d, 100d, 1000d));
//    private static final DataPointGroup SINGLE_DOUBLE_GROUP =
//            createGroup(new DoubleDataPoint(1L, 0d));
//    private static final DataPointGroup EMPTY_HIST_GROUP = createGroup(createHistogram(1L));
//    private static final DataPointGroup MULTI_HIST_GROUP = createGroup(
//            createHistogram(1L, 10d, 20d, 30d, 40d),
//            createHistogram(2L, 20d, 30d, 40d, 50d),
//            createHistogram(3L, 30d, 40d, 50d, 60d)
//    );

    private DataPointGroup SINGLE_HIST_GROUP;
    private DataPointGroup SINGLE_DOUBLE_GROUP;
    private DataPointGroup EMPTY_HIST_GROUP;
    private DataPointGroup MULTI_HIST_GROUP;

    private void runPercentRemainingTest(final DataPointGroup startGroup, final DataPointGroup expected,
                                         final Aggregator... aggregators) {
        DataPointGroup aggregated = startGroup;

        for (Aggregator agg : aggregators) {
            aggregated = agg.aggregate(aggregated);
        }

        final DataPointGroup percentRemainingGroup = _percentRemainingAggregator.aggregate(aggregated);

        while (expected.hasNext()) {
            Assert.assertTrue(percentRemainingGroup.hasNext());
            final DataPoint actual = percentRemainingGroup.next();
            Assert.assertEquals(expected.next(), actual);
        }
        Assert.assertFalse(percentRemainingGroup.hasNext());
    }

    private static DataPointGroup createDoubleGroup(final double... values) {
        final ListDataPointGroup group = new ListDataPointGroup("test_values");
        long ts = 1L;
        for (final double value : values) {
            group.addDataPoint(new DoubleDataPoint(ts++, value));
        }
        return group;
    }

    private void configureFilter(final FilterAggregator.FilterOperation op, final double threshold) {
        _filterAggregator.setThreshold(threshold);
        _filterAggregator.setFilterOp(op);
    }

    @Before
    public void setUp() throws KairosDBException {
        _percentRemainingAggregator = new HistogramPercentRemainingAggregator(new DoubleDataPointFactoryImpl());

        _filterAggregator = new HistogramFilterAggregator();
        _filterAggregator.setFilterIndeterminateInclusion(HistogramFilterAggregator.FilterIndeterminate.DISCARD);

        _mergeAggregator = new HistogramMergeAggregator();
        _mergeAggregator.setStartTime(1L);
        _mergeAggregator.setSampling(new Sampling(10, TimeUnit.MINUTES));

        SINGLE_HIST_GROUP = createGroup(createHistogram(1L, 1d, 10d, 100d, 1000d));
        SINGLE_DOUBLE_GROUP = createGroup(new DoubleDataPoint(1L, 0d));;
        EMPTY_HIST_GROUP = createGroup(createHistogram(1L));
        MULTI_HIST_GROUP = createGroup(
                createHistogram(1L, 10d, 20d, 30d, 40d),
                createHistogram(2L, 20d, 30d, 40d, 50d),
                createHistogram(3L, 30d, 40d, 50d, 60d)
        );
    }

    @Test(expected = NullPointerException.class)
    public void testPercentRemainingNull() {
        _percentRemainingAggregator.aggregate(null);
    }

    @Test
    public void testPercentRemainingEmptyGroup() {
        runPercentRemainingTest(EMPTY_HIST_GROUP, createDoubleGroup(-1d), _filterAggregator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentRemainingNotHistograms() {
        final DataPointGroup percentRemainingGroup = _percentRemainingAggregator.aggregate(SINGLE_DOUBLE_GROUP);
        Assert.assertTrue(percentRemainingGroup.hasNext());
        percentRemainingGroup.next();
    }

    @Test
    public void testPercentRemainingFilterAllOut() {
        configureFilter(FilterAggregator.FilterOperation.GT, 0d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(0d), _filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterNoneOut() {
        configureFilter(FilterAggregator.FilterOperation.GT, 10000d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(1d), _filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterLT() {
        configureFilter(FilterAggregator.FilterOperation.LT, 5d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(0.75), _filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterLTE() {
        configureFilter(FilterAggregator.FilterOperation.LTE, 10d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(0.5), _filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterGT() {
        configureFilter(FilterAggregator.FilterOperation.GT, 5d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(0.25), _filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterGTE() {
        configureFilter(FilterAggregator.FilterOperation.GTE, 100d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(0.5), _filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterEqual() {
        configureFilter(FilterAggregator.FilterOperation.EQUAL, 100d);
        runPercentRemainingTest(SINGLE_HIST_GROUP, createDoubleGroup(0.75), _filterAggregator);
    }

    @Test
    public void testPercentRemainingMerge() {
        runPercentRemainingTest(MULTI_HIST_GROUP, createDoubleGroup(1d), _mergeAggregator);
    }

    @Test
    public void testPercentRemainingMergeThenFilter() {
        configureFilter(FilterAggregator.FilterOperation.LTE, 30d);
        runPercentRemainingTest(MULTI_HIST_GROUP, createDoubleGroup(0.5), _mergeAggregator, _filterAggregator);

    }

    @Test
    public void testPercentRemainingFilterThenMerge() {
        configureFilter(FilterAggregator.FilterOperation.LTE, 30d);
        runPercentRemainingTest(MULTI_HIST_GROUP, createDoubleGroup(0.5), _filterAggregator, _mergeAggregator);

    }
}
