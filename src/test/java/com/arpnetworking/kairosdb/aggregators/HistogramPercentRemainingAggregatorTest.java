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
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.exception.KairosDBException;

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
    private DataPointGroup _group;
    
    private static final DataPointGroup SINGLE_HIST_GROUP = 
            createGroup(createHistogram(1L, 1d, 10d, 100d, 1000d));
    private static final DataPointGroup SINGLE_DOUBLE_GROUP =
            createGroup(new DoubleDataPoint(1L, 0d));
    private static final DataPointGroup EMPTY_HIST_GROUP = createGroup(createHistogram(1L));
    private static final DataPointGroup MULTI_HIST_GROUP = createGroup(
            createHistogram(1L, 10d, 20d, 30d, 40d),
            createHistogram(2L, 20d, 30d, 40d, 50d),
            createHistogram(3L, 30d, 40d, 50d, 60d)
    );

    private void runPercentRemainingFilterTest(final double percentRemaining) {
        final DataPointGroup remainingGroup = _filterAggregator.aggregate(_group);
        final DataPointGroup percentRemainingGroup = _percentRemainingAggregator.aggregate(remainingGroup);
        final DataPoint expected = new DoubleDataPoint(1L, percentRemaining);
        Assert.assertTrue(percentRemainingGroup.hasNext());
        final DataPoint actual = percentRemainingGroup.next();
        Assert.assertEquals(expected, actual);
        Assert.assertFalse(percentRemainingGroup.hasNext());
    }

    @Before
    public void setUp() throws KairosDBException {
        _percentRemainingAggregator = new HistogramPercentRemainingAggregator(new DoubleDataPointFactoryImpl());

        _filterAggregator = new HistogramFilterAggregator();
        _filterAggregator.setFilterIndeterminateInclusion(HistogramFilterAggregator.FilterIndeterminate.DISCARD);

        _mergeAggregator = new HistogramMergeAggregator();
    }

    @Test(expected = NullPointerException.class)
    public void testPercentRemainingNull() {
        _percentRemainingAggregator.aggregate(null);
    }

    @Test
    public void testPercentRemainingEmptyGroup() {
        _group = EMPTY_HIST_GROUP;
        runPercentRemainingFilterTest(-1d);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentRemainingNotHistograms() {
        _group = SINGLE_DOUBLE_GROUP;
        final DataPointGroup percentRemainingGroup = _percentRemainingAggregator.aggregate(_group);
        Assert.assertTrue(percentRemainingGroup.hasNext());
        percentRemainingGroup.next();
    }

    @Test
    public void testPercentRemainingFilterAll() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(0d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GT);
        runPercentRemainingFilterTest(0d);
    }

    @Test
    public void testPercentRemainingFilterNone() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(10000d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GT);
        runPercentRemainingFilterTest(1d);
    }

    @Test
    public void testPercentRemainingFilterLT() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(5d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.LT);
        runPercentRemainingFilterTest(0.75);
    }

    @Test
    public void testPercentRemainingFilterLTE() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(10d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.LTE);
        runPercentRemainingFilterTest(0.5);
    }

    @Test
    public void testPercentRemainingFilterGT() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(5d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GT);
        runPercentRemainingFilterTest(0.25);
    }

    @Test
    public void testPercentRemainingFilterGTE() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(100d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.GTE);
        runPercentRemainingFilterTest(0.5d);
    }

    @Test
    public void testPercentRemainingFilterEqual() {
        _group = SINGLE_HIST_GROUP;
        _filterAggregator.setThreshold(100d);
        _filterAggregator.setFilterOp(FilterAggregator.FilterOperation.EQUAL);
        runPercentRemainingFilterTest(0.75d);
    }

    @Test
    public void testPercentRemainingMerge() {
        //TODO
        _group = MULTI_HIST_GROUP;

    }

    @Test
    public void testPercentRemainingMergeThenFilter() {
        //TODO
        _group = MULTI_HIST_GROUP;
    }

    @Test
    public void testPercentRemainingFilterThenMerge() {
        //TODO
        _group = MULTI_HIST_GROUP;
    }
}
