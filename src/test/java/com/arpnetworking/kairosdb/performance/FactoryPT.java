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
package com.arpnetworking.kairosdb.performance;

import com.arpnetworking.kairosdb.HistogramFormingMetrics;
import com.arpnetworking.metrics.generator.metric.GaussianCountMetricGenerator;
import com.arpnetworking.metrics.generator.metric.GaussianMetricGenerator;
import com.arpnetworking.metrics.generator.name.SingleNameGenerator;
import com.google.gson.JsonObject;
import org.apache.commons.math3.random.MersenneTwister;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * Base class for serialization tests.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public abstract class FactoryPT {
    @Test
    public void serializeSize() throws IOException {
        final int elements = 10000;
        final MersenneTwister random = new MersenneTwister(0);
        final GaussianMetricGenerator gaussianMetricGenerator =
                new GaussianMetricGenerator(300, 90, new SingleNameGenerator(random), random);
        final GaussianCountMetricGenerator generator =
                new GaussianCountMetricGenerator(20000, 12000, gaussianMetricGenerator, random);

        final HistogramFormingMetrics metrics = new HistogramFormingMetrics(7);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataOutputStream outputStream = new DataOutputStream(out);

        final DataPointFactory factory = getFactory();
        for (int x = 0; x < elements; x++) {
            generator.generate(metrics);
            final JsonObject json = new JsonObject();
            json.addProperty("min", metrics.getMin());
            json.addProperty("max", metrics.getMax());
            json.addProperty("sum", metrics.getSum());
            json.addProperty("mean", metrics.getMean());
            final JsonObject bins = new JsonObject();
            for (Map.Entry<Double, Integer> entry : metrics.getHistogram().entrySet()) {
                bins.addProperty(entry.getKey().toString(), entry.getValue());
            }
            json.add("bins", bins);
            json.addProperty("precision", metrics.getPrecision());

            final DataPoint dataPoint = factory.getDataPoint(ZonedDateTime.now().toEpochSecond() * 1000, json);

            dataPoint.writeValueToBuffer(outputStream);
        }
        out.close();
        System.out.println(factory.getClass().getSimpleName() + " total Bytes: " + out.size());
    }

    protected abstract DataPointFactory getFactory();
}
