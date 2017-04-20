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

import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Helper class to hold the histogram information for testing.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@SuppressFBWarnings("FE_FLOATING_POINT_EQUALITY")
public class Histogram {
    /**
     * Public constructor.
     *
     * @param numbers The raw numbers.
     */
    public Histogram(final Iterable<Double> numbers) {
        _min = Double.MAX_VALUE;
        _max = -Double.MAX_VALUE;
        _sum = 0;
        _count = 0;
        for (Double number : numbers) {
            _sum += number;
            _min = Math.min(_min, number);
            _max = Math.max(_max, number);
            _bins.compute(Math.floor(number), (i, j) -> j == null ? 1 : j + 1);
            _count++;
        }
    }

    /**
     * Public constructor.
     *
     * @param json The JSON node to parse.
     * @throws JSONException on any JSON processing errors.
     */
    @SuppressWarnings("unchecked")
    public Histogram(final JSONObject json) throws JSONException {
        _min = json.getDouble("min");
        _max = json.getDouble("max");
        _sum = json.getDouble("sum");
        final JSONObject binsJson = json.getJSONObject("bins");
        for (final Iterator<String> it = (Iterator<String>) binsJson.keys(); it.hasNext();) {
            final String key = it.next();
            final int value = binsJson.getInt(key);
            _bins.put(Double.valueOf(key), value);
            _count += value;

        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj.getClass().equals(getClass())) {
            final Histogram other = (Histogram) obj;
            return other._bins.equals(_bins)
                    && other._count == _count
                    && other._max == _max
                    && other._min == _min
                    && other._sum == _sum;
        } else {
            return false;
        }

    }

    @Override
    public int hashCode() {
        return Objects.hash(_bins, _count, _min, _max, _sum);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("min", _min)
                .add("max", _max)
                .add("sum", _sum)
                .add("count", _count)
                .add("bins", _bins)
                .toString();
    }

    /**
     * Gets a JSON representation of the histogram.
     *
     * @return a new JSONObject
     * @throws JSONException on JSON errors
     */
    public JSONObject getJson() throws JSONException {
        final JSONObject histogram = new JSONObject();
        histogram.put("bins", _bins)
                .put("mean", _sum / _count)
                .put("min", _min)
                .put("max", _max)
                .put("sum", _sum);
        return histogram;
    }

    public double getMean() {
        return _sum / _count;
    }

    public double getMin() {
        return _min;
    }

    public double getMax() {
        return _max;
    }

    public double getSum() {
        return _sum;
    }

    public TreeMap<Double, Integer> getBins() {
        return _bins;
    }

    private double _min;
    private double _max;
    private double _sum;
    private int _count;

    private final TreeMap<Double, Integer> _bins = new TreeMap<>();
}
