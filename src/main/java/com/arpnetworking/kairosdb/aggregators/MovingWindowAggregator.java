/**
 * Copyright 2018 Dropbox Inc.
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
import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GregorianChronology;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.AggregatedDataPointGroupWrapper;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Aggregator that takes a window of datapoints and emits them to downstream aggregators as if they came from the
 * end time of the window.  This allows for generically doing things like a moving average or percentile over a 
 * moving/rolling period of time.
 * 
 * @author Gil Markham (gmarkham@dropbox.com)
 */
@FeatureComponent(name = "movingWindow", description = "Creates a moving window of datapoints")
public class MovingWindowAggregator extends RangeAggregator {
    private long _startTime = 0L;
    private DateTimeZone _timeZone = DateTimeZone.UTC;
    private boolean _alignSampling = false;

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        Preconditions.checkNotNull(dataPointGroup);

        if (_alignSampling) {
            _startTime = alignRangeBoundary(_startTime);
        }
        return new MovingWindowDataPointGroup(dataPointGroup);
    }

    /**
     * Calculates the beginning of a window for the provided startTime given the sampling and
     * alignment parameters of this aggregator.
     *
     * @param startTime Time to use for calculating the window start
     * @return The window start time for the provided startTime
     */
    public long calculateEarliestStartTime(final long startTime) {
        long tmpStartTime = startTime;
        if (_alignSampling) {
           tmpStartTime = alignRangeBoundary(tmpStartTime);
        }
        return timeUnitToTimeField(m_sampling.getUnit()).add(tmpStartTime, -1 * m_sampling.getValue());
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return true;
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return groupType;
    }

    @Override
    public void setStartTime(final long startTime) {
        _startTime = startTime;
        super.setStartTime(startTime);
    }

    @Override
    public void setTimeZone(final DateTimeZone timeZone) {
        _timeZone = timeZone;
        super.setTimeZone(timeZone);
    }

    @Override
    public void setAlignSampling(final boolean alignSampling) {
        _alignSampling = alignSampling;
        super.setAlignSampling(alignSampling);
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return null;
    }

    /**
     * For YEARS, MONTHS, WEEKS, DAYS: Computes the timestamp of the first
     * millisecond of the day of the timestamp. For HOURS, Computes the timestamp of
     * the first millisecond of the hour of the timestamp. For MINUTES, Computes the
     * timestamp of the first millisecond of the minute of the timestamp. For
     * SECONDS, Computes the timestamp of the first millisecond of the second of the
     * timestamp. For MILLISECONDS, returns the timestamp
     *
     * @param timestamp Timestamp in milliseconds to use as a basis for range alignment
     * @return timestamp aligned to the configured sampling unit
     */
    @SuppressWarnings("fallthrough")
    @SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
    private long alignRangeBoundary(final long timestamp) {
        DateTime dt = new DateTime(timestamp, _timeZone);
        final TimeUnit tu = m_sampling.getUnit();
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
                dt = dt.withHourOfDay(0);
            case HOURS:
                dt = dt.withMinuteOfHour(0);
            case MINUTES:
                dt = dt.withSecondOfMinute(0);
            case SECONDS:
            default:
                dt = dt.withMillisOfSecond(0);
                break;
        }
        return dt.getMillis();
    }

    private DateTimeField timeUnitToTimeField(final TimeUnit timeUnit) {
        final Chronology chronology = GregorianChronology.getInstance(_timeZone);
        switch (timeUnit) {
            case YEARS:
                return chronology.year();
            case MONTHS:
                return chronology.monthOfYear();
            case WEEKS:
                return chronology.weekOfWeekyear();
            case DAYS:
                return chronology.dayOfMonth();
            case HOURS:
                return chronology.hourOfDay();
            case MINUTES:
                return chronology.minuteOfHour();
            case SECONDS:
                return chronology.secondOfDay();
            default:
                return chronology.millisOfSecond();
        }
    }

    private class MovingWindowDataPointGroup extends AggregatedDataPointGroupWrapper {
        private DateTimeField _unitField;
        private final LinkedList<DataPoint> _dpBuffer;
        private int _currentIndex;
        private int _lastTruncatedIndex;
        private TreeMap<Long, Integer> _timestampIndexMap;
        private Iterator<DataPoint> _dpIterator;
        private long _currentStartRange;
        private long _currentEndRange;

        MovingWindowDataPointGroup(final DataPointGroup dataPointGroup) {
            super(dataPointGroup);
            _dpBuffer = new LinkedList<>();
            _dpIterator = _dpBuffer.iterator();
            _currentIndex = 0;
            _lastTruncatedIndex = 0;
            _timestampIndexMap = new TreeMap<>();
            _unitField = timeUnitToTimeField(m_sampling.getUnit());
        }

        protected long getStartRange(final long timestamp) {
            final long samplingValue = m_sampling.getValue();
            long difference = _unitField.getDifferenceAsLong(timestamp, _startTime);
            if (_unitField.remainder(timestamp - _startTime) > 0) {
                difference += 1;
            }
            return _unitField.add(_startTime, difference + -1 * samplingValue);
        }

        protected long getEndRange(final long timestamp) {
            // Subract one millisecond off the resulting time, so the endRange doesn't overlap
            // with the next startRange
            return _unitField.add(getStartRange(timestamp), 1) - 1;
        }

        @Override
        public DataPoint next() {
            if (_dpBuffer.isEmpty() || !_dpIterator.hasNext()) {

                // We calculate start and end ranges as the ranges may not be
                // consecutive if data does not show up in each range.
                if (_dpBuffer.isEmpty()) {
                    // Get the greater of the start values given we may not have datapoints for the
                    // beginning of the range and we want the current datapoint to fit in the first
                    // time range
                    final long mwStartTime = calculateEarliestStartTime(_startTime);
                    final long dpStartRange = getStartRange(currentDataPoint.getTimestamp());
                    _currentStartRange = mwStartTime > dpStartRange ? mwStartTime : dpStartRange;
                } else {
                    // Move the window forward one unit
                    _currentStartRange = _unitField.add(_currentStartRange, 1);

                    // Trim off any data that is too old
                    final Map.Entry<Long, Integer> floorEntry = _timestampIndexMap.floorEntry(_currentStartRange);
                    if (floorEntry != null) {
                        _dpBuffer.subList(0, floorEntry.getValue() - _lastTruncatedIndex + 1).clear();
                        _lastTruncatedIndex = floorEntry.getValue() + 1;
                        _timestampIndexMap.headMap(floorEntry.getKey(), true).clear();
                    }

                    if (_dpBuffer.isEmpty()) {
                        // We have no datapoints in the current window so we need to jump the window
                        // forward to include the next available datapoint
                        _currentStartRange = getStartRange(currentDataPoint.getTimestamp());
                    }
                }
                _currentEndRange = _unitField.add(_currentStartRange, m_sampling.getValue());

                // Add data up until the end of the range
                while (currentDataPoint != null && currentDataPoint.getTimestamp() <= _currentEndRange) {
                    if (currentDataPoint.getTimestamp() > _currentStartRange) {
                        _timestampIndexMap.put(currentDataPoint.getTimestamp(), _currentIndex++);
                        _dpBuffer.add(currentDataPoint);
                    }
                    if (hasNextInternal()) {
                        currentDataPoint = nextInternal();
                    }
                }

                _dpIterator = _dpBuffer.iterator();
            }


            final DataPoint next = _dpIterator.next();
            if (next instanceof HistogramDataPoint) {
                return new HistogramDataPointWrapper(_currentEndRange, (HistogramDataPoint) next);
            } else {
                return new DataPointWrapper(_currentEndRange, next);
            }
        }

        @Override
        public boolean hasNext() {
            return _dpIterator.hasNext() || super.hasNext();
        }
    }

    /**
     * Class to wrap a Datapoint so that the timestamp can be masked and overwritten.
     */
    protected static class DataPointWrapper implements DataPoint {
        private DataPoint _wrappedDataPoint;
        private long _timestamp;

        DataPointWrapper(final long timestamp, final DataPoint dataPoint) {
            _timestamp = timestamp;
            _wrappedDataPoint = dataPoint;
        }

        @Override
        public long getTimestamp() {
            return _timestamp;
        }

        @Override
        public void writeValueToBuffer(final DataOutput buffer) throws IOException {
            _wrappedDataPoint.writeValueToBuffer(buffer);
        }

        @Override
        public void writeValueToJson(final JSONWriter writer) throws JSONException {
            _wrappedDataPoint.writeValueToJson(writer);
        }

        @Override
        public String getApiDataType() {
            return _wrappedDataPoint.getApiDataType();
        }

        @Override
        public String getDataStoreDataType() {
            return _wrappedDataPoint.getDataStoreDataType();
        }

        @Override
        public boolean isLong() {
            return _wrappedDataPoint.isLong();
        }

        @Override
        public long getLongValue() {
            return _wrappedDataPoint.getLongValue();
        }

        @Override
        public boolean isDouble() {
            return _wrappedDataPoint.isDouble();
        }

        @Override
        public double getDoubleValue() {
            return _wrappedDataPoint.getDoubleValue();
        }

        @Override
        public DataPointGroup getDataPointGroup() {
            return _wrappedDataPoint.getDataPointGroup();
        }

        @Override
        public void setDataPointGroup(final DataPointGroup dataPointGroup) {
            _wrappedDataPoint.setDataPointGroup(dataPointGroup);
        }
    }

    /**
     * Class to wrap a Datapoint so that the timestamp can be masked and overwritten.
     */
    protected static class HistogramDataPointWrapper implements HistogramDataPoint {
        private HistogramDataPoint _wrappedDataPoint;
        private long _timestamp;

        HistogramDataPointWrapper(final long timestamp, final HistogramDataPoint dataPoint) {
            _timestamp = timestamp;
            _wrappedDataPoint = dataPoint;
        }

        @Override
        public long getTimestamp() {
            return _timestamp;
        }

        @Override
        public void writeValueToBuffer(final DataOutput buffer) throws IOException {
            _wrappedDataPoint.writeValueToBuffer(buffer);
        }

        @Override
        public void writeValueToJson(final JSONWriter writer) throws JSONException {
            _wrappedDataPoint.writeValueToJson(writer);
        }

        @Override
        public String getApiDataType() {
            return _wrappedDataPoint.getApiDataType();
        }

        @Override
        public String getDataStoreDataType() {
            return _wrappedDataPoint.getDataStoreDataType();
        }

        @Override
        public boolean isLong() {
            return _wrappedDataPoint.isLong();
        }

        @Override
        public long getLongValue() {
            return _wrappedDataPoint.getLongValue();
        }

        @Override
        public boolean isDouble() {
            return _wrappedDataPoint.isDouble();
        }

        @Override
        public double getDoubleValue() {
            return _wrappedDataPoint.getDoubleValue();
        }

        @Override
        public DataPointGroup getDataPointGroup() {
            return _wrappedDataPoint.getDataPointGroup();
        }

        @Override
        public void setDataPointGroup(final DataPointGroup dataPointGroup) {
            _wrappedDataPoint.setDataPointGroup(dataPointGroup);
        }

        @Override
        public int getOriginalCount() {
            return _wrappedDataPoint.getOriginalCount();
        }

        @Override
        public int getSampleCount() {
            return _wrappedDataPoint.getSampleCount();
        }

        @Override
        public double getSum() {
            return _wrappedDataPoint.getSum();
        }

        @Override
        public double getMin() {
            return _wrappedDataPoint.getMin();
        }

        @Override
        public double getMax() {
            return _wrappedDataPoint.getMax();
        }

        @Override
        public NavigableMap<Double, Integer> getMap() {
            return _wrappedDataPoint.getMap();
        }
    }
}
