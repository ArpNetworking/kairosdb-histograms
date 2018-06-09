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

import com.arpnetworking.kairosdb.proto.v2.DataPointV2;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Factory that creates {@link HistogramDataPointV1Impl}.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class HistogramDataPointV2Factory implements DataPointFactory {
    /**
     * Name of the Data Store Type.
     */
    public static final String DST = "kairos_histogram_v2_proto";
    /**
     * Name of the group type.
     */
    public static final String GROUP_TYPE = "histogram";

    private static final Logger LOGGER = LoggerFactory.getLogger(HistogramDataPointV2Impl.class);

    /**
     * Default constructor.
     */
    public HistogramDataPointV2Factory() { }

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

        final byte precision = Optional.ofNullable(object.get("precision")).map(JsonElement::getAsByte).orElse((byte) 7);
        return new HistogramDataPointV2Impl(timestamp, precision, binValues, min, max, mean, sum);
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final DataInput buffer) throws IOException {
//        LOGGER.warn("reading HDPF from byte buffer");
        final int length = buffer.readInt();
        final byte[] data = new byte[length];
        buffer.readFully(data, 0, data.length);

        final DataPointV2.DataPoint protoData = DataPointV2.DataPoint.parseFrom(data);

        final int precision = protoData.getPrecision();
        final TreeMap<Double, Integer> bins = protoData.getHistogramMap()
                .entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                entry -> HistogramDataPointV2Impl.unpack(entry.getKey(), precision),
                                Map.Entry::getValue,
                                (u, v) -> {
                                    throw new IllegalArgumentException(String.format("Duplicate key %s", u));
                                },
                                Maps::newTreeMap));

        final double min = protoData.getMin();
        final double max = protoData.getMax();
        final double mean = protoData.getMean();
        final double sum = protoData.getSum();

        final HistogramDataPointV2Impl histogramDataPointV2 = new HistogramDataPointV2Impl(timestamp, precision, bins, min, max, mean, sum);
//        LOGGER.warn("created HDPF: " + histogramDataPointV2.toString());
        return histogramDataPointV2;
    }

//    private Double expandPacked(final int packed) {
//        return HistogramDataPointV2Impl.u
//        return Double.longBitsToDouble(((long) packed) << 32);
//    }
}
