/*
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
    private final TreeMap<Double, Integer> bins = new TreeMap<>();
    private double min;
    private double max;
    private double sum;
    private int count;

    /**
     * Public constructor.
     *
     * @param numbers The raw numbers.
     */
    public Histogram(final Iterable<Double> numbers) {
        min = Double.MAX_VALUE;
        max = -Double.MAX_VALUE;
        sum = 0;
        count = 0;
        for (Double number : numbers) {
            sum += number;
            min = Math.min(min, number);
            max = Math.max(max, number);
            bins.compute(Math.floor(number), (i, j) -> j == null ? 1 : j + 1);
            count++;
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
        min = json.getDouble("min");
        max = json.getDouble("max");
        sum = json.getDouble("sum");
        final JSONObject binsJson = json.getJSONObject("bins");
        for (final Iterator<String> it = (Iterator<String>) binsJson.keys(); it.hasNext();) {
            final String key = it.next();
            final int value = binsJson.getInt(key);
            bins.put(Double.valueOf(key), value);
            count += value;

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
            return other.bins.equals(bins)
                    && other.count == count
                    && other.max == max
                    && other.min == min
                    && other.sum == sum;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(bins, count, min, max, sum);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("min", min)
                .add("max", max)
                .add("sum", sum)
                .add("count", count)
                .add("bins", bins)
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
        histogram.put("bins", bins)
                .put("mean", sum / count)
                .put("min", min)
                .put("max", max)
                .put("sum", sum);
        return histogram;
    }

    public double getMean() {
        return sum / count;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getSum() {
        return sum;
    }

    public TreeMap<Double, Integer> getBins() {
        return bins;
    }
}
