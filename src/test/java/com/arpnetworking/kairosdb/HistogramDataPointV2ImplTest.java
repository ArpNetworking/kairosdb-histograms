/**
 * Copyright 2019 InscopeMetrics Inc
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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

/**
 * Tests for the HistogramDataPointV2Impl class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HistogramDataPointV2ImplTest {

    @Test
    public void packAndUnpack() {
        final int precision = 7;
        final HistogramFormingMetrics metrics = new HistogramFormingMetrics(precision);
        for (double i = -1000; i < 1000; i += 0.01) {
            metrics.setGauge("", i);
        }
        final HistogramDataPointV2Impl histogram = new HistogramDataPointV2Impl(
                4982,
                precision,
                new TreeMap<>(metrics.getHistogram()),
                metrics.getMin(),
                metrics.getMax(),
                metrics.getMean(),
                metrics.getSum());

        for (final Map.Entry<Double, Integer> entry : histogram.getMap().entrySet()) {
            final long packed = histogram.pack(entry.getKey());
            final double unpacked = HistogramDataPointV2Impl.unpack(packed, precision);
            Assert.assertEquals(Double.doubleToLongBits(entry.getKey()), Double.doubleToLongBits(unpacked));
        }
    }

}
