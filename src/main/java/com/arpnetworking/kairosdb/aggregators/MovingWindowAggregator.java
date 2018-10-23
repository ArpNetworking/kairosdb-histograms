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

import com.google.common.base.Preconditions;
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
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Aggregator that takes a window of datapoints and emits them to downstream aggregators as if they came from the
 * end time of the window.  This allows for generically doing things like a moving average or percentile over a 
 * moving/rolling period of time.
 * 
 * @author Gil Markham (gmarkham@dropbox.com)
 */
@FeatureComponent(name = "movingWindow", description = "Creates a moving window of datapoints")
public class MovingWindowAggregator extends RangeAggregator {
    protected long _startTime = 0L;
    protected DateTimeZone _timeZone = DateTimeZone.UTC;
    protected boolean _alignSampling = false;

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        Preconditions.checkNotNull(dataPointGroup);

        if (_alignSampling) {
            _startTime = alignRangeBoundary(_startTime);
        }
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
     * @param timestamp Timestamp in milliseconds to use as a basis for range alignment
     * @return timestamp aligned to the configured sampling unit
     */
    @SuppressWarnings("fallthrough")
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

    private class MovingWindowDataPointGroup extends AggregatedDataPointGroupWrapper {
        protected DateTimeField _unitField;
        protected ConcurrentSkipListMap<Long, DataPoint> _dpBuffer;
        protected Iterator<DataPoint> _dpIterator = null;
        protected long _currentRangeTime;

        MovingWindowDataPointGroup(final DataPointGroup dataPointGroup) {
            super(dataPointGroup);
            _dpBuffer = new ConcurrentSkipListMap<>();
            _dpIterator = _dpBuffer.values().iterator();

            final Chronology chronology = GregorianChronology.getInstance(_timeZone);

            final TimeUnit tu = m_sampling.getUnit();
            switch (tu) {
            case YEARS:
                _unitField = chronology.year();
                break;
            case MONTHS:
                _unitField = chronology.monthOfYear();
                break;
            case WEEKS:
                _unitField = chronology.weekOfWeekyear();
                break;
            case DAYS:
                _unitField = chronology.dayOfMonth();
                break;
            case HOURS:
                _unitField = chronology.hourOfDay();
                break;
            case MINUTES:
                _unitField = chronology.minuteOfHour();
                break;
            case SECONDS:
                _unitField = chronology.secondOfDay();
                break;
            default:
                _unitField = chronology.millisOfSecond();
                break;
            }
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
                final long startRange; 

                if (_dpBuffer.isEmpty()) {
                    // This is the first datapoint so front-load the sample window amount of data.
                    startRange = _startTime;
                } else {
                    startRange = getStartRange(currentDataPoint.getTimestamp());
                }

                final long endRange = _unitField.add(startRange, m_sampling.getValue());
                _currentRangeTime = endRange;

                // Trim off any data that is too old
                _dpBuffer = new ConcurrentSkipListMap<>(_dpBuffer.tailMap(startRange, false));

                // Add data up until the end of the range
                while (currentDataPoint != null && currentDataPoint.getTimestamp() <= endRange) {
                    if (currentDataPoint.getTimestamp() > startRange) {
                        _dpBuffer.put(currentDataPoint.getTimestamp(), currentDataPoint);
                    }
                    if (hasNextInternal()) {
                        currentDataPoint = nextInternal();
                    }
                }

                _dpIterator = _dpBuffer.values().iterator();
            }

            return new DataPointWrapper(_currentRangeTime, _dpIterator.next());
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
        protected DataPoint _wrappedDataPoint;
        protected long _timestamp;

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
}
