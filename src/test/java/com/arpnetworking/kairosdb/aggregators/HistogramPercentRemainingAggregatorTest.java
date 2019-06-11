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

import com.arpnetworking.kairosdb.HistogramDataPoint;
import com.arpnetworking.kairosdb.HistogramDataPointImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Map;
import java.util.TreeMap;

/**
 * Unit tests for the Histogram Percent Remaining Aggregator.
 *
 * @author Joey Jackson
 */
public class HistogramPercentRemainingAggregatorTest {

    private HistogramPercentRemainingAggregator percentRemainingAggregator;
    private HistogramFilterAggregator filterAggregator;
    private HistogramMergeAggregator mergeAggregator;
    private ListDataPointGroup group;

    private ListDataPointGroup createHistogramGroup() {
        final ListDataPointGroup group = new ListDataPointGroup("test_values");

        final TreeMap<Double, Integer> bins1 = new TreeMap<>();
        bins1.put();
        bins1.put();
        bins1.put();
        group.addDataPoint(createHistogram(1L, bins1));

        final TreeMap<Double, Integer> bins2 = new TreeMap<>();
        bins2.put();
        bins2.put();
        bins2.put();
        group.addDataPoint(createHistogram(2L, bins2));

        return group;
    }

    @Before
    public void setUp() throws KairosDBException {
        percentRemainingAggregator = new HistogramPercentRemainingAggregator(new DoubleDataPointFactoryImpl());
        filterAggregator = new HistogramFilterAggregator();
        mergeAggregator = new HistogramMergeAggregator();

        group = createHistogramGroup();
    }

    @Test(expected = NullPointerException.class)
    public void testPercentRemainingNull() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingEmptyGroup() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterAll() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterNone() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterLT() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterLTE() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterGT() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterGTE() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingFilterEqual() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingMergeAll() {
        //TODO
        Assert.assertTrue(false);
    }

    @Test
    public void testPercentRemainingMergeSome() {
        //TODO
        Assert.assertTrue(false);
    }
}
