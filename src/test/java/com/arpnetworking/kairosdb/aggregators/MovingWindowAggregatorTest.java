package com.arpnetworking.kairosdb.aggregators;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MovingWindowAggregatorTest {

    @Test
	public void test_multipleMonthAggregationWithUTC()
	{
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_group");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2014, 1, 1, 1, 1, utc); // LEAP year
		DateTime stopDate = new DateTime(2014, 7, 1, 1, 1, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
        }
        
		MovingWindowAggregator agg = new MovingWindowAggregator();
		agg.setSampling(new Sampling(3, TimeUnit.MONTHS));
		agg.setAlignSampling(true);
		agg.setStartTime(startDate.getMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);
        
        SortedMap<Long, Long> dpCountMap = new TreeMap<>();

        while(dpg.hasNext()) {
            DataPoint next = dpg.next();
            dpCountMap.compute(next.getTimestamp(), (key, value) -> {
                if (value == null) {
                    return 1L;
                } else {
                    return value + 1;
                }
            });
        }

        Iterator<Long> keyIter = dpCountMap.keySet().iterator();
        assertThat(keyIter.hasNext(), is(true));
		Long nextTs = keyIter.next();
		assertThat(new DateTime(nextTs, utc), is(new DateTime(2014, 4, 1, 0, 0, utc)));
		assertThat(dpCountMap.get(nextTs), is(90L)); // 31 + 28 + 31

        assertThat(keyIter.hasNext(), is(true));
		nextTs = keyIter.next();
		assertThat(new DateTime(nextTs, utc), is(new DateTime(2014, 5, 1, 0, 0, utc)));
		assertThat(dpCountMap.get(nextTs), is(89L)); // 28 + 31 + 30

        assertThat(keyIter.hasNext(), is(true));
		nextTs = keyIter.next();
		assertThat(new DateTime(nextTs, utc), is(new DateTime(2014, 6, 1, 0, 0, utc)));
		assertThat(dpCountMap.get(nextTs), is(92L)); // 31 + 30 + 31
        
        assertThat(keyIter.hasNext(), is(true));
		nextTs = keyIter.next();
		assertThat(new DateTime(nextTs, utc), is(new DateTime(2014, 7, 1, 0, 0, utc)));
		assertThat(dpCountMap.get(nextTs), is(91L)); // 30 + 31 + 30
		
		assertThat(keyIter.hasNext(), is(false));
	}

	@Test
	public void test_multipleRolling7DayAggregationWithUTC()
	{
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_group");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2018, 1, 1, 1, 1, utc); 
		DateTime stopDate = new DateTime(2018, 1, 30, 1, 1, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
        }
        
		MovingWindowAggregator agg = new MovingWindowAggregator();
		agg.setSampling(new Sampling(7, TimeUnit.DAYS));
		agg.setAlignSampling(true);
		agg.setStartTime(startDate.getMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);
        
        SortedMap<Long, Long> dpCountMap = new TreeMap<>();

        while(dpg.hasNext()) {
            DataPoint next = dpg.next();
            dpCountMap.compute(next.getTimestamp(), (key, value) -> {
                if (value == null) {
                    return 1L;
                } else {
                    return value + 1;
                }
            });
        }

		Iterator<Long> keyIter = dpCountMap.keySet().iterator();
		for (int i = 8; i <= 30; i++) {
			assertThat(keyIter.hasNext(), is(true));
			Long nextTs = keyIter.next();
			assertThat(new DateTime(nextTs, utc), is(new DateTime(2018, 1, i, 0, 0, utc)));
			assertThat(dpCountMap.get(nextTs), is(7L)); 
		}
	}

	@Test
	public void test_multipleUnalignedRolling7DayAggregationWithUTC()
	{
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_group");
		DateTimeZone utc = DateTimeZone.UTC;

		DateTime startDate = new DateTime(2018, 1, 1, 1, 1, utc); 
		DateTime stopDate = new DateTime(2018, 1, 30, 1, 1, utc);
		for (DateTime iterationDT = startDate;
		     iterationDT.isBefore(stopDate);
		     iterationDT = iterationDT.plusDays(1))
		{
			dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
        }
        
		MovingWindowAggregator agg = new MovingWindowAggregator();
		agg.setSampling(new Sampling(7, TimeUnit.DAYS));
		agg.setAlignSampling(false);
		agg.setStartTime(startDate.getMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);
        
        SortedMap<Long, Long> dpCountMap = new TreeMap<>();

        while(dpg.hasNext()) {
            DataPoint next = dpg.next();
            dpCountMap.compute(next.getTimestamp(), (key, value) -> {
                if (value == null) {
                    return 1L;
                } else {
                    return value + 1;
                }
            });
        }

		Iterator<Long> keyIter = dpCountMap.keySet().iterator();
		for (int i = 8; i <= 29; i++) {
			assertThat(keyIter.hasNext(), is(true));
			Long nextTs = keyIter.next();
			assertThat(new DateTime(nextTs, utc), is(new DateTime(2018, 1, i, 1, 1, utc)));
			assertThat(dpCountMap.get(nextTs), is(7L)); 
		}
	}
}