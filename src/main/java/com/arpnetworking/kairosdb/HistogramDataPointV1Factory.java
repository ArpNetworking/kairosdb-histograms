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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointFactory;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Factory that creates {@link HistogramDataPointV1Impl}.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class HistogramDataPointV1Factory implements DataPointFactory {
    /**
     * Name of the Data Store Type.
     */
    public static final String DST = "kairos_histogram_v1";
    /**
     * Name of the group type.
     */
    public static final String GROUP_TYPE = "histogram";

    /**
     * Default constructor.
     */
    public HistogramDataPointV1Factory() { }

    @Override
    public String getDataStoreType() {
        return DST;
    }

    @Override
    public String getGroupType() {
        return GROUP_TYPE;
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final JsonElement json) {
        final TreeMap<Double, Integer> binValues = new TreeMap<>();

        final JsonObject object = json.getAsJsonObject();
        final double min = object.get("min").getAsDouble();
        final double max = object.get("max").getAsDouble();
        final double mean = object.get("mean").getAsDouble();
        final double sum = object.get("sum").getAsDouble();
        final JsonObject bins = object.get("bins").getAsJsonObject();

        for (Map.Entry<String, JsonElement> entry : bins.entrySet()) {
            binValues.put(Double.parseDouble(entry.getKey()), entry.getValue().getAsInt());
        }

        return new HistogramDataPointV1Impl(timestamp, 7, binValues, min, max, mean, sum);
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final DataInput buffer) throws IOException {
        final TreeMap<Double, Integer> bins = new TreeMap<>();
        final int binCount = buffer.readInt();
        for (int i = 0; i < binCount; i++) {
            bins.put(buffer.readDouble(), buffer.readInt());
        }

        final double min = buffer.readDouble();
        final double max = buffer.readDouble();
        final double mean = buffer.readDouble();
        final double sum = buffer.readDouble();
        final int precision = 7;

        return new HistogramDataPointV1Impl(timestamp, precision, bins, min, max, mean, sum);
    }
}
