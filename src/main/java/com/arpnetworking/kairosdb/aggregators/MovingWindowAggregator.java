package com.arpnetworking.kairosdb.aggregators;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GregorianChronology;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.AggregatedDataPointGroupWrapper;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.aggregator.RangeAggregator;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

@FeatureComponent(name = "movingWindow", description = "Creates a moving window of datapoints")
public class MovingWindowAggregator extends RangeAggregator {
    protected long m_startTime = 0L;
    protected DateTimeZone m_timeZone = DateTimeZone.UTC;
    protected boolean m_alignSampling = false;

    public MovingWindowAggregator() {

    }

    @Override
    public DataPointGroup aggregate(DataPointGroup dataPointGroup) {
        checkNotNull(dataPointGroup);

        if (m_alignSampling)
            m_startTime = alignRangeBoundary(m_startTime);
        return new MovingWindowDataPointGroup(dataPointGroup);
    }

    /**
     * For YEARS, MONTHS, WEEKS, DAYS: Computes the timestamp of the first
     * millisecond of the day of the timestamp. For HOURS, Computes the timestamp of
     * the first millisecond of the hour of the timestamp. For MINUTES, Computes the
     * timestamp of the first millisecond of the minute of the timestamp. For
     * SECONDS, Computes the timestamp of the first millisecond of the second of the
     * timestamp. For MILLISECONDS, returns the timestamp
     *
     * @param timestamp
     * @return
     */
    // @SuppressWarnings("fallthrough")
    private long alignRangeBoundary(long timestamp) {
        DateTime dt = new DateTime(timestamp, m_timeZone);
        TimeUnit tu = m_sampling.getUnit();
        switch (tu) {
        case YEARS:
            dt = dt.withDayOfYear(1).withMillisOfDay(0);
            break;
        case MONTHS:
            dt = dt.withDayOfMonth(1).withMillisOfDay(0);
            break;
        case WEEKS:
            dt = dt.withDayOfWeek(1).withMillisOfDay(0);
            break;
        case DAYS:
        case HOURS:
        case MINUTES:
        case SECONDS:
            dt = dt.withHourOfDay(0);
            dt = dt.withMinuteOfHour(0);
            dt = dt.withSecondOfMinute(0);
        default:
            dt = dt.withMillisOfSecond(0);
            break;
        }
        return dt.getMillis();
    }

    @Override
    public boolean canAggregate(String groupType) {
        return true;
    }

    @Override
    public String getAggregatedGroupType(String groupType) {
        return groupType;
    }

    @Override
    public void setStartTime(long startTime) {
        m_startTime = startTime;
        super.setStartTime(startTime);
    }

    @Override
    public void setTimeZone(DateTimeZone timeZone) {
        m_timeZone = timeZone;
        super.setTimeZone(timeZone);
    }

    @Override
    public void setAlignSampling(boolean alignSampling) {
        m_alignSampling = alignSampling;
        super.setAlignSampling(alignSampling);
    }

    private class MovingWindowDataPointGroup extends AggregatedDataPointGroupWrapper {
        protected DateTimeField m_unitField;
        protected ConcurrentSkipListMap<Long, DataPoint> m_dpBuffer;
        protected Iterator<DataPoint> m_dpIterator = null;
        protected long m_currentRangeTime;

        MovingWindowDataPointGroup(DataPointGroup dataPointGroup) {
            super(dataPointGroup);
            m_dpBuffer = new ConcurrentSkipListMap<>();
            m_dpIterator = m_dpBuffer.values().iterator();

            Chronology chronology = GregorianChronology.getInstance(m_timeZone);

            TimeUnit tu = m_sampling.getUnit();
            switch (tu) {
            case YEARS:
                m_unitField = chronology.year();
                break;
            case MONTHS:
                m_unitField = chronology.monthOfYear();
                break;
            case WEEKS:
                m_unitField = chronology.weekOfWeekyear();
                break;
            case DAYS:
                m_unitField = chronology.dayOfMonth();
                break;
            case HOURS:
                m_unitField = chronology.hourOfDay();
                break;
            case MINUTES:
                m_unitField = chronology.minuteOfHour();
                break;
            case SECONDS:
                m_unitField = chronology.secondOfDay();
                break;
            default:
                m_unitField = chronology.millisOfSecond();
                break;
            }
        }

        protected long getStartRange(long timestamp) {
            long samplingValue = m_sampling.getValue();
            long difference = m_unitField.getDifferenceAsLong(timestamp, m_startTime);
            if (m_unitField.remainder(timestamp - m_startTime) > 0) {
                difference += 1;
            }
            return m_unitField.add(m_startTime, difference + -1 * samplingValue);
        }

        protected long getEndRange(long timestamp) {
            long difference = m_unitField.getDifferenceAsLong(timestamp, m_startTime);
            return m_unitField.add(m_startTime, difference) - 1;
        }

        @Override
        public DataPoint next() {
            if (m_dpBuffer.isEmpty() || !m_dpIterator.hasNext()) {

                // We calculate start and end ranges as the ranges may not be
                // consecutive if data does not show up in each range.
                long startRange; 

                if (m_dpBuffer.isEmpty()) {
                    // This is the first datapoint so front-load the sample window amount of data.
                    startRange = m_startTime;
                } else {
                    startRange = getStartRange(currentDataPoint.getTimestamp());
                }

                long endRange = m_unitField.add(startRange, m_sampling.getValue());
                m_currentRangeTime = endRange;

                // Trim off any data that is too old
                m_dpBuffer = new ConcurrentSkipListMap<>(m_dpBuffer.tailMap(startRange, false));

                // Add data up until the end of the range
                while (currentDataPoint != null && currentDataPoint.getTimestamp() <= endRange) {
                    if (currentDataPoint.getTimestamp() > startRange) {
                        m_dpBuffer.put(currentDataPoint.getTimestamp(), currentDataPoint);
                    }
                    if (hasNextInternal()) {
                        currentDataPoint = nextInternal();
                    }
                }

                m_dpIterator = m_dpBuffer.values().iterator();
            }

            return new DataPointWrapper(m_currentRangeTime, m_dpIterator.next());
        }

        @Override
        public boolean hasNext() {
            return m_dpIterator.hasNext() || super.hasNext();
        }

        /**
         * Computes the data point time for the aggregated value. Different strategies
         * could be added here such as datapoint time = range start time = range end
         * time = range median = current datapoint time
         *
         * @return
         */
        private long getDataPointTime() {
            long datapointTime = currentDataPoint.getTimestamp();
            if (m_alignStartTime) {
                datapointTime = getStartRange(datapointTime);
            } else if (m_alignEndTime) {
                datapointTime = getEndRange(datapointTime);
            }
            return datapointTime;
        }
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return null;
    }

    protected static class DataPointWrapper implements DataPoint {
        protected DataPoint m_wrappedDataPoint;
        protected long m_timestamp;

        DataPointWrapper(long timestamp, DataPoint dataPoint) {
            m_timestamp = timestamp;
            m_wrappedDataPoint = dataPoint;
        }

        @Override
        public long getTimestamp() {
            return m_timestamp;
        }

        @Override
        public void writeValueToBuffer(DataOutput buffer) throws IOException {
            m_wrappedDataPoint.writeValueToBuffer(buffer);
        }

        @Override
        public void writeValueToJson(JSONWriter writer) throws JSONException {
            m_wrappedDataPoint.writeValueToJson(writer);
        }

        @Override
        public String getApiDataType() {
            return m_wrappedDataPoint.getApiDataType();
        }

        @Override
        public String getDataStoreDataType() {
            return m_wrappedDataPoint.getDataStoreDataType();
        }

        @Override
        public boolean isLong() {
            return m_wrappedDataPoint.isLong();
        }

        @Override
        public long getLongValue() {
            return m_wrappedDataPoint.getLongValue();
        }

        @Override
        public boolean isDouble() {
            return m_wrappedDataPoint.isDouble();
        }

        @Override
        public double getDoubleValue() {
            return m_wrappedDataPoint.getDoubleValue();
        }

        @Override
        public DataPointGroup getDataPointGroup() {
            return m_wrappedDataPoint.getDataPointGroup();
        }

        @Override
        public void setDataPointGroup(DataPointGroup dataPointGroup) {
            m_wrappedDataPoint.setDataPointGroup(dataPointGroup);
        }
    }
}