/**
 * Copyright 2018 Inscope Metrics, Inc
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

import com.arpnetworking.metrics.Counter;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.Timer;
import com.arpnetworking.metrics.Unit;
import com.google.common.collect.Maps;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Metrics implementation that pulls all data into a single histogram.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HistogramFormingMetrics implements Metrics {
    /**
     * Public constructor.
     *
     * @param precision precision of the bins in the histogram, in bits
     */
    public HistogramFormingMetrics(final int precision) {
        _precision = precision;
    }

    @Override
    public Counter createCounter(final String name) {
        return null;
    }

    @Override
    public void incrementCounter(final String name) {
        throw new RuntimeException();
    }

    @Override
    public void incrementCounter(final String name, final long value) {
        throw new RuntimeException();
    }

    @Override
    public void decrementCounter(final String name) {
        throw new RuntimeException();

    }

    @Override
    public void decrementCounter(final String name, final long value) {
        throw new RuntimeException();
    }

    @Override
    public void resetCounter(final String name) {
        throw new RuntimeException();
    }

    @Override
    public Timer createTimer(final String name) {
        return null;
    }

    @Override
    public void startTimer(final String name) {
        throw new RuntimeException();

    }

    @Override
    public void stopTimer(final String name) {
        throw new RuntimeException();

    }

    @Override
    @SuppressWarnings("deprecation")
    public void setTimer(final String name, final long duration, @Nullable final TimeUnit unit) {
        recordValue(duration, 1);
    }

    @Override
    public void setTimer(final String name, final long duration, @Nullable final Unit unit) {
        recordValue(duration, 1);
    }

    @Override
    public void setGauge(final String name, final double value) {
        recordValue(value, 1);
    }

    @Override
    public void setGauge(final String name, final double value, @Nullable final Unit unit) {
        recordValue(value, 1);
    }

    @Override
    public void setGauge(final String name, final long value) {
        throw new RuntimeException();
    }

    @Override
    public void setGauge(final String name, final long value, @Nullable final Unit unit) {
        throw new RuntimeException();
    }

    @Override
    public void addAnnotation(final String key, final String value) {
    }

    @Override
    public void addAnnotations(final Map<String, String> map) {
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() {
    }

    @Nullable
    @Override
    public Instant getOpenTime() {
        return null;
    }

    @Nullable
    @Override
    public Instant getCloseTime() {
        return null;
    }

    public Map<Double, Integer> getHistogram() {
        return _histogram;
    }

    public int getPrecision() {
        return _precision;
    }

    public Double getSum() {
        return _sum;
    }

    public Double getMax() {
        return _max;
    }

    public Double getMin() {
        return _min;
    }

    public Double getMean() {
        return _sum / _count;
    }

    private double truncate(final double val) {
        final long mask = 0xfff0000000000000L >> _precision;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(val) & mask);
    }

    private void recordValue(final double value, final int count) {
        _histogram.merge(truncate(value), count, (i, j) -> i + j);
        if (_min == null || value < _min) {
            _min = value;
        }
        if (_max == null || value > _max) {
            _max = value;
        }
        _count += count;
        _sum += value;
    }

    private final int _precision;
    private final Map<Double, Integer> _histogram = Maps.newHashMap();
    private Double _sum = 0d;
    private Double _max;
    private Double _min;
    private int _count;

}
