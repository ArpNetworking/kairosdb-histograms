package org.kairosdb.testing;

import com.arpnetworking.kairosdb.HistogramDataPoint;
import com.arpnetworking.kairosdb.HistogramDataPointImpl;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;

import java.util.TreeMap;

public class HistogramUtils {

    public static void assertHistogramGroupsEqual(final DataPointGroup expected, final DataPointGroup actual) {
        while (expected.hasNext()) {
            Assert.assertTrue("Actual group is missing data points", actual.hasNext());
            final DataPoint act = actual.next();
            final DataPoint exp = expected.next();
            Assert.assertEquals("Expected and actual timestamps do not match", act.getTimestamp(),
                    exp.getTimestamp());
            assertHistogramsEqual(exp, act);
        }
        Assert.assertFalse("Actual group has too many data points", actual.hasNext());
    }

    public static void assertHistogramsEqual(final DataPoint expected, final DataPoint actual) {
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

    public static ListDataPointGroup createGroup(final DataPoint... dataPoints) {
        final ListDataPointGroup group = new ListDataPointGroup("test_values");
        for (DataPoint dp : dataPoints) {
            group.addDataPoint(dp);
        }
        return group;
    }

    public static HistogramDataPoint createHistogram(final long timeStamp, final Double... binValues) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double sum = 0;
        double count = 0;
        final TreeMap<Double, Integer> bins = Maps.newTreeMap();

        for (final Double binValue : binValues) {
            final int binCount = 10;
            sum += binCount * binValue;
            min = Math.min(min, binValue);
            max = Math.max(max, binValue);
            count += binCount;
            bins.put(binValue, binCount);
        }
        final double mean = sum / count;

        return new HistogramDataPointImpl(timeStamp, 7, bins, min, max, mean, sum);
    }

}
