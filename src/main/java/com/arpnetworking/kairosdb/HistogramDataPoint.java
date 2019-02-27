/*
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
package com.arpnetworking.kairosdb;

import org.kairosdb.core.DataPoint;

import java.util.NavigableMap;

/**
 * DataPoint that represents a Histogram.
 *
 * @author Gil Markham (gmarkham at dropbox dot com)
 */
public interface HistogramDataPoint extends DataPoint {
    /**
     * Getter for sample count contained in this HistogramDataPoint.
     * @return datapoint sample count
     */
    int getSampleCount();

    /**
     * Getter for the sum of all samples contained in this HistogramDataPoint.
     * @return sum of histogram samples
     */
    double getSum();

    /**
     * Getter for the min value of the samples contained in this HistogramDataPoint.
     * @return min of histogram samples
     */
    double getMin();

    /**
     * Getter for the max value of the samples contained in this HistogramDataPoint.
     * @return max of histogram samples
     */
    double getMax();

    /**
     * Getter for the precision value of the samples contained in this HistogramDataPoint.
     * @return precision in bits
     */
    int getPrecision();

    /**
     * Getter for the map of lower bucket value to sample count contained in this HistogramDataPoint.
     * @return map of histogram buckets
     */
    NavigableMap<Double, Integer> getMap();

    /**
     * Truncates a value to the specified bits of precision.
     * @param val value to truncate
     * @param precision bits of precision
     * @return truncated value
     */
    static double truncate(final double val, final int precision) {
        final long mask = 0xfff0000000000000L >> precision;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(val) & mask);
    }
}
