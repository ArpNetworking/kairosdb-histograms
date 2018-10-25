/**
 * Copyright 2017 SmartSheet.com
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
package com.arpnetworking.kairosdb;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datapoints.DataPointHelper;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * DataPoint that represents a Histogram.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class HistogramDataPointImpl extends DataPointHelper implements HistogramDataPoint {
    private static final String API_TYPE = "histogram";
    private final int _precision;
    private final TreeMap<Double, Integer> _map;
    private final double _min;
    private final double _max;
    private final double _mean;
    private final double _sum;

    /**
     * Public constructor.
     *
     * @param timestamp the timestamp.
     * @param precision bucket precision, in bits
     * @param map the bins with values
     * @param min the minimum value in the histogram
     * @param max the maximum value in the histogram
     * @param mean the mean value in the histogram
     * @param sum the sum of all the values in the histogram
     */
    public HistogramDataPointImpl(final long timestamp, final int precision, final TreeMap<Double, Integer> map, final double min,
                                  final double max, final double mean, final double sum) {
        super(timestamp);
        _precision = precision;
        _map = map;
        _min = min;
        _max = max;
        _mean = mean;
        _sum = sum;
    }

    @Override
    public void writeValueToBuffer(final DataOutput buffer) throws IOException {
        buffer.writeInt(_map.size());
        for (Map.Entry<Double, Integer> entry : _map.entrySet()) {
            buffer.writeDouble(entry.getKey());
            buffer.writeInt(entry.getValue());
        }
        buffer.writeDouble(_min);
        buffer.writeDouble(_max);
        buffer.writeDouble(_mean);
        buffer.writeDouble(_sum);
    }

    @Override
    public void writeValueToJson(final JSONWriter writer) throws JSONException {
        writer.object().key("bins");
        writer.object();
        for (Map.Entry<Double, Integer> entry : _map.entrySet()) {
            writer.key(entry.getKey().toString()).value(entry.getValue());
        }
        writer.endObject();
        writer.key("min").value(_min);
        writer.key("max").value(_max);
        writer.key("mean").value(_mean);
        writer.key("sum").value(_sum);
        writer.endObject();
    }

    @Override
    public String getApiDataType() {
        return API_TYPE;
    }

    @Override
    public String getDataStoreDataType() {
        return HistogramDataPointFactory.DST;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public long getLongValue() {
        return 0;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public double getDoubleValue() {
        return 0;
    }

    /**
     * Gets the number of samples in the bins.
     *
     * @return the number of samples
     */
    @Override
    public int getSampleCount() {
        int count = 0;
        for (Integer binSamples : _map.values()) {
            count += binSamples;
        }
        return count;
    }

    @Override
    public double getSum() {
        return _sum;
    }

    @Override
    public double getMin() {
        return _min;
    }

    @Override
    public double getMax() {
        return _max;
    }

    @Override
    public TreeMap<Double, Integer> getMap() {
        return _map;
    }
}
