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

import com.arpnetworking.kairosdb.HistogramDataPointImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.HistogramUtils;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.TreeMap;

/**
 * Unit tests for the Histogram Filter Aggregator.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class HistogramFilterAggregatorTest {
    private HistogramFilterAggregator _aggregator;

    private static final double NEG_516_0 = Double.longBitsToDouble(0xc080200000000000L);  //-516.0
    private static final double NEG_512_0 = Double.longBitsToDouble(0xc080000000000000L);  //-512.0
    private static final double NEG_100_5 = Double.longBitsToDouble(0xc059200000000000L);  //-100.5
    private static final double NEG_100_01 = Double.longBitsToDouble(0xc0590000FFFFFFFFL); //-100.01
    private static final double NEG_100_0 = Double.longBitsToDouble(0xc059000000000000L);  //-100.0
    private static final double NEG_99_5 = Double.longBitsToDouble(0xc058e00000000000L);   // -99.5
    private static final double NEG_1EN308 = Double.longBitsToDouble(0x8000200000000000L); //  -1.e-308
    private static final double NEG_0_0 = Double.longBitsToDouble(0x8000000000000000L);    //  -0.0
    private static final double POS_0_0 = Double.longBitsToDouble(0x0000000000000000L);    //   0.0
    private static final double POS_1EN308 = Double.longBitsToDouble(0x0000200000000000L); //   1.e-308
    private static final double POS_99_5 = Double.longBitsToDouble(0x4058e00000000000L);   //  99.5
    private static final double POS_100_0 = Double.longBitsToDouble(0x4059000000000000L);  // 100.0
    private static final double POS_100_01 = Double.longBitsToDouble(0x40590000FFFFFFFFL); // 100.01
    private static final double POS_100_5 = Double.longBitsToDouble(0x4059200000000000L);  // 100.6
    private static final double POS_512_0 = Double.longBitsToDouble(0x4080000000000000L);  // 512.0
    private static final double POS_516_0 = Double.longBitsToDouble(0x4080200000000000L);  // 516.0

    @Before
    public void setUp() {
        _aggregator = new HistogramFilterAggregator();
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNull() {
        _aggregator.aggregate(null);
    }

    @Test
    public void testFilterEmptyGroup() {
        final DataPointGroup group = HistogramUtils.createGroup();
        final DataPointGroup results = _aggregator.aggregate(group);
        Assert.assertFalse("Actual group was not empty", results.hasNext());
    }

    @Test
    public void testFilterNotHistogramDataPoint() {
        final ListDataPointGroup group = HistogramUtils.createGroup(
                new DoubleDataPoint(1L, 100.0),
                new DoubleDataPoint(2L, 100.0)
        );

        final ListDataPointGroup expected = HistogramUtils.createGroup(
                new HistogramDataPointImpl(1L, 7, new TreeMap<>(),
                        Double.NaN, Double.NaN, Double.NaN, Double.NaN),
                new HistogramDataPointImpl(2L, 7, new TreeMap<>(),
                        Double.NaN, Double.NaN, Double.NaN, Double.NaN)
        );

        final DataPointGroup results = _aggregator.aggregate(group);
        HistogramUtils.assertHistogramGroupsEqual(expected, results);
    }

    @Test
    public void testFilterRemoveAllBins() {
        final ListDataPointGroup group = HistogramUtils.createGroup(
                HistogramUtils.createHistogram(1L, POS_100_0, POS_512_0, POS_516_0));

        _aggregator.setFilterOp(FilterAggregator.FilterOperation.GTE);
        _aggregator.setFilterIndeterminateInclusion(HistogramFilterAggregator.FilterIndeterminate.KEEP);
        _aggregator.setThreshold(POS_0_0);

        final ListDataPointGroup expected = HistogramUtils.createGroup(
                new HistogramDataPointImpl(1L, 7, new TreeMap<>(),
                        Double.NaN, Double.NaN, Double.NaN, Double.NaN)
        );
        final DataPointGroup results = _aggregator.aggregate(group);
        HistogramUtils.assertHistogramGroupsEqual(expected, results);
    }

    @Test
    public void testFilterAroundZero() {
        DataPointGroup group, expected, results;
        _aggregator.setFilterIndeterminateInclusion(HistogramFilterAggregator.FilterIndeterminate.KEEP);

        group = HistogramUtils.createGroup(
                HistogramUtils.createHistogram(1L, POS_1EN308, POS_0_0, NEG_0_0, NEG_1EN308));
        _aggregator.setFilterOp(FilterAggregator.FilterOperation.LTE);
        _aggregator.setThreshold(POS_0_0);
        expected = HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_1EN308, POS_0_0));
        results = _aggregator.aggregate(group);
        HistogramUtils.assertHistogramGroupsEqual(expected, results);

        group = HistogramUtils.createGroup(
                HistogramUtils.createHistogram(1L, POS_1EN308, POS_0_0, NEG_0_0, NEG_1EN308));
        _aggregator.setFilterOp(FilterAggregator.FilterOperation.GTE);
        _aggregator.setThreshold(NEG_0_0);
        expected = HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_0_0, NEG_1EN308));
        results = _aggregator.aggregate(group);
        HistogramUtils.assertHistogramGroupsEqual(expected, results);
    }

    @Test
    public void testFilterLessThanKeepThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true, 
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanKeepThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false, 
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_0),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanKeepThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanKeepThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_0),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanDiscardThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanDiscardThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanDiscardThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanDiscardThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanOrEqualKeepThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanOrEqualKeepThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanOrEqualKeepThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanOrEqualKeepThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_0),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanOrEqualDiscardThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanOrEqualDiscardThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterLessThanOrEqualDiscardThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterLessThanOrEqualDiscardThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0)));
    }

    @Test
    public void testFilterGreaterThanKeepThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_0),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanKeepThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanKeepThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_0),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanKeepThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanDiscardThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanDiscardThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanDiscardThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanDiscardThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualKeepThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualKeepThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualKeepThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_0),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualKeepThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualDiscardThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualDiscardThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualDiscardThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5),
                        HistogramUtils.createHistogram(2L, POS_0_0)));
    }

    @Test
    public void testFilterGreaterThanOrEqualDiscardThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterEqualKeepThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_0_0, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterEqualKeepThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterEqualKeepThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_0, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_0_0, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterEqualKeepThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_0, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterEqualDiscardThresholdAtBinBoundaryPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_0_0, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterEqualDiscardThresholdAtBinBoundaryNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0, NEG_512_0, NEG_516_0)));
    }

    @Test
    public void testFilterEqualDiscardThresholdMiddleOfBinPositiveBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, POS_99_5, POS_100_5),
                        HistogramUtils.createHistogram(2L, POS_0_0, POS_512_0, POS_516_0)));
    }

    @Test
    public void testFilterEqualDiscardThresholdMiddleOfBinNegativeBins() {
        runFilterTest(FilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false,
                HistogramUtils.createGroup(HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_5),
                        HistogramUtils.createHistogram(2L, NEG_0_0, NEG_512_0, NEG_516_0)));
    }

    private void runFilterTest(final FilterAggregator.FilterOperation op,
                               final HistogramFilterAggregator.FilterIndeterminate ind,
                               final boolean isAtBoundaryTest, final boolean isPositiveTest,
                               final ListDataPointGroup expected) {
        _aggregator.setFilterOp(op);
        _aggregator.setFilterIndeterminateInclusion(ind);
        final ListDataPointGroup group;
        if (isPositiveTest) {
            group = HistogramUtils.createGroup(
                    HistogramUtils.createHistogram(1L, POS_99_5, POS_100_0, POS_100_5),
                    HistogramUtils.createHistogram(2L, POS_0_0, POS_512_0, POS_516_0));
            if (isAtBoundaryTest) {
                _aggregator.setThreshold(POS_100_0);
            } else {
                _aggregator.setThreshold(POS_100_01);
            }
        } else {
            group = HistogramUtils.createGroup(
                    HistogramUtils.createHistogram(1L, NEG_99_5, NEG_100_0, NEG_100_5),
                    HistogramUtils.createHistogram(2L, NEG_0_0, NEG_512_0, NEG_516_0));
            if (isAtBoundaryTest) {
                _aggregator.setThreshold(NEG_100_0);
            } else {
                _aggregator.setThreshold(NEG_100_01);
            }
        }
        final DataPointGroup results = _aggregator.aggregate(group);
        HistogramUtils.assertHistogramGroupsEqual(expected, results);
    }


}
