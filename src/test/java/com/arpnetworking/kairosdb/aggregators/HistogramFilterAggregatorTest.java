package com.arpnetworking.kairosdb.aggregators;

import com.arpnetworking.kairosdb.HistogramDataPoint;
import com.arpnetworking.kairosdb.HistogramDataPointImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Map;
import java.util.TreeMap;

public class HistogramFilterAggregatorTest {
    private HistogramFilterAggregator aggregator;

    private final static double THRESHOLD_POSITIVE_AT_BOUNDARY = Double.longBitsToDouble(0x4059000000000000L); //100.0
    private final static double THRESHOLD_POSITIVE_NOT_AT_BOUNDARY = Double.longBitsToDouble(0x40590000FFFFFFFFL); //100.01
    private final static double THRESHOLD_NEGATIVE_AT_BOUNDARY = Double.longBitsToDouble(0xc059000000000000L); //-100.0
    private final static double THRESHOLD_NEGATIVE_NOT_AT_BOUNDARY = Double.longBitsToDouble(0xc0590000FFFFFFFFL); //-100.01

    private void assertHistogramsEqual(final DataPoint expected, final DataPoint actual) {
        Assert.assertTrue("Data point not an instance of class HistogramDataPoint",
                expected instanceof HistogramDataPoint);
        Assert.assertTrue("Data point not an instance of class HistogramDataPoint",
                actual instanceof HistogramDataPoint);
        final HistogramDataPoint hist1 = (HistogramDataPoint) expected;
        final HistogramDataPoint hist2 = (HistogramDataPoint) actual;

        Assert.assertEquals("Histograms did not match", hist1.getMap(), hist2.getMap());
        Assert.assertEquals(hist1.getSampleCount(), hist2.getSampleCount());
        Assert.assertEquals(hist1.getSum(), hist2.getSum(), 0);
        Assert.assertEquals(hist1.getMin(), hist2.getMin(), 0);
        Assert.assertEquals(hist1.getMax(), hist2.getMax(), 0);
    }

    private HistogramDataPoint createHistogram(long timeStamp, final TreeMap<Double, Integer> bins) {
        double _min = Double.MAX_VALUE;
        double _max = -Double.MAX_VALUE;
        double _sum = 0;
        double _count = 0;
        for (final Map.Entry<Double, Integer> entry : bins.entrySet()) {
            _sum += entry.getValue() * entry.getKey();
            _min = Math.min(_min, entry.getKey());
            _max = Math.max(_max, entry.getKey());
            _count++;
        }
        double _mean = _sum / _count;

        return new HistogramDataPointImpl(timeStamp, 7, bins, _min, _max, _mean, _sum);
    }

    private ListDataPointGroup createPositiveDataPointGroup() {
        ListDataPointGroup group = new ListDataPointGroup("test_values");
        final TreeMap<Double, Integer> bins1 = new TreeMap<>();
        bins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        bins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        bins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        group.addDataPoint(createHistogram(1L, bins1));
        final TreeMap<Double, Integer> bins2 = new TreeMap<>();
        bins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        bins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        bins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        group.addDataPoint(createHistogram(2L, bins2));

        return group;
    }

    private ListDataPointGroup createNegativeDataPointGroup() {
        ListDataPointGroup group = new ListDataPointGroup("test_values");
        final TreeMap<Double, Integer> bins1 = new TreeMap<>();
        bins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        bins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        bins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        group.addDataPoint(createHistogram(1L, bins1));
        final TreeMap<Double, Integer> bins2 = new TreeMap<>();
        bins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        bins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        bins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        group.addDataPoint(createHistogram(2L, bins2));

        return group;
    }

    private void runTest(HistogramFilterAggregator.FilterOperation op, HistogramFilterAggregator.FilterIndeterminate ind,
                         boolean isAtBoundaryTest, boolean isPositiveTest, ListDataPointGroup expected) {
        aggregator.setFilterOp(op);
        aggregator.setInclusion(ind);
        ListDataPointGroup group;
        if (isPositiveTest) {
            group = createPositiveDataPointGroup();
            if (isAtBoundaryTest) {
                aggregator.setThreshold(THRESHOLD_POSITIVE_AT_BOUNDARY);
            } else {
                aggregator.setThreshold(THRESHOLD_POSITIVE_NOT_AT_BOUNDARY);
            }
        } else {
            group = createNegativeDataPointGroup();
            if (isAtBoundaryTest) {
                aggregator.setThreshold(THRESHOLD_NEGATIVE_AT_BOUNDARY);
            } else {
                aggregator.setThreshold(THRESHOLD_NEGATIVE_NOT_AT_BOUNDARY);
            }
        }
        DataPointGroup results = aggregator.aggregate(group);
        while (expected.hasNext()) {
            Assert.assertTrue("Aggregated group is missing a data point", results.hasNext());
            DataPoint act = results.next();
            DataPoint exp = expected.next();
            Assert.assertEquals("Expected timestamp was different than actual", act.getTimestamp(),
                    exp.getTimestamp());
            assertHistogramsEqual(exp, act);
        }
        Assert.assertFalse("Results had an extra data point", results.hasNext());
    }

    @Before
    public void setUp() {
        aggregator = new HistogramFilterAggregator();
    }

    @Test(expected = NullPointerException.class)
    public void testFilterNull() {
        aggregator.aggregate(null);
    }

    @Test
    public void testFilterEmptyGroup() {
        DataPointGroup group = new ListDataPointGroup("test_empty_group");

        DataPointGroup results = aggregator.aggregate(group);
        Assert.assertFalse("Actual group was not empty", results.hasNext());
    }

    @Test
    public void testFilterNotHistogramDataPoint() {
        ListDataPointGroup group = new ListDataPointGroup("test_not_histogram_group");
        group.addDataPoint(new DoubleDataPoint(1L, 100.0));
        group.addDataPoint(new DoubleDataPoint(2L, 100.0));

        ListDataPointGroup expected = new ListDataPointGroup("test_expected");
        expected.addDataPoint(new HistogramDataPointImpl(1L, 7, new TreeMap<>(),
                Double.MAX_VALUE, -Double.MAX_VALUE, 0, 0));
        expected.addDataPoint(new HistogramDataPointImpl(2L, 7, new TreeMap<>(),
                Double.MAX_VALUE, -Double.MAX_VALUE, 0, 0));

        DataPointGroup results = aggregator.aggregate(group);
        while (expected.hasNext()) {
            Assert.assertTrue("Aggregated group is missing a data point", results.hasNext());
            DataPoint act = results.next();
            DataPoint exp = expected.next();
            Assert.assertEquals("Expected timestamp was different than actual", act.getTimestamp(),
                    exp.getTimestamp());
            assertHistogramsEqual(exp, act);
        }
        Assert.assertFalse("Results had an extra data point", results.hasNext());
    }

    @Test
    public void testFilter_LessThan_Keep_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true, expected);
    }

    @Test
    public void testFilter_LessThan_Keep_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false, expected);
    }

    @Test
    public void testFilter_LessThan_Keep_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true, expected);
    }

    @Test
    public void testFilter_LessThan_Keep_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false, expected);
    }

    @Test
    public void testFilter_LessThan_Discard_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true, expected);
    }

    @Test
    public void testFilter_LessThan_Discard_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false, expected);
    }

    @Test
    public void testFilter_LessThan_Discard_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true, expected);
    }

    @Test
    public void testFilter_LessThan_Discard_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Keep_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Keep_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Keep_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Keep_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Discard_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Discard_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Discard_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true, expected);
    }

    @Test
    public void testFilter_LessThanOrEqual_Discard_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.LTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false, expected);
    }

    @Test
    public void testFilter_GreaterThan_Keep_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true, expected);
    }

    @Test
    public void testFilter_GreaterThan_Keep_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false, expected);
    }

    @Test
    public void testFilter_GreaterThan_Keep_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true, expected);
    }

    @Test
    public void testFilter_GreaterThan_Keep_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false, expected);
    }

    @Test
    public void testFilter_GreaterThan_Discard_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true, expected);
    }

    @Test
    public void testFilter_GreaterThan_Discard_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false, expected);
    }

    @Test
    public void testFilter_GreaterThan_Discard_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true, expected);
    }

    @Test
    public void testFilter_GreaterThan_Discard_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GT, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Keep_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Keep_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Keep_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Keep_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Discard_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Discard_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Discard_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true, expected);
    }

    @Test
    public void testFilter_GreaterThanOrEqual_Discard_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
//        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.GTE, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false, expected);
    }

    @Test
    public void testFilter_Equal_Keep_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, true, expected);
    }

    @Test
    public void testFilter_Equal_Keep_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                true, false, expected);
    }

    @Test
    public void testFilter_Equal_Keep_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, true, expected);
    }

    @Test
    public void testFilter_Equal_Keep_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.KEEP,
                false, false, expected);
    }

    @Test
    public void testFilter_Equal_Discard_ThresholdAtBinBoundary_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, true, expected);
    }

    @Test
    public void testFilter_Equal_Discard_ThresholdAtBinBoundary_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                true, false, expected);
    }

    @Test
    public void testFilter_Equal_Discard_ThresholdMiddleOfBin_PositiveBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4058e00000000000L)), 13); // 99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059000000000000L)), 17); //100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x4059200000000000L)), 53); //100.6
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x0000000000000000L)), 3); //  0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059000000000000L)), 7); //1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x5059200000000000L)), 5); //1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, true, expected);
    }

    @Test
    public void testFilter_Equal_Discard_ThresholdMiddleOfBin_NegativeBins() {
        ListDataPointGroup expected = new ListDataPointGroup("test_values_expected");
        final TreeMap<Double, Integer> expectedBins1 = new TreeMap<>();
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc058e00000000000L)), 13); // -99.5
//        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059000000000000L)), 17); //-100.0
        expectedBins1.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xc059200000000000L)), 53); //-100.5
        expected.addDataPoint(createHistogram(1L, expectedBins1));
        final TreeMap<Double, Integer> expectedBins2 = new TreeMap<>();
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0x8000000000000000L)), 3); //  -0.0
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059000000000000L)), 7); //-1.e79
        expectedBins2.put(HistogramFilterAggregator.truncate(Double.longBitsToDouble(0xd059200000000000L)), 5); //-1.e79
        expected.addDataPoint(createHistogram(2L, expectedBins2));

        runTest(HistogramFilterAggregator.FilterOperation.EQUAL, HistogramFilterAggregator.FilterIndeterminate.DISCARD,
                false, false, expected);
    }

}
