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

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;

/**
 * Utility functions for building KairosDB queries and objects.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class KairosHelper {
    private KairosHelper() { }

    /**
     * Creates the JSON body for a datapoint POST.
     *
     * @param timestamp timestamp of the datapoint
     * @param histogram the histogram
     * @param metricName the name of the metric
     * @return JSON Array to POST to KairosDB
     * @throws JSONException on a JSON error
     */
    public static HttpPost postHistogram(final int timestamp, final Histogram histogram, final String metricName) throws JSONException {
        final JSONArray json = new JSONArray();
        final JSONObject metric = new JSONObject();
        final JSONArray datapoints = new JSONArray();
        final JSONArray datapoint = new JSONArray();
        final JSONObject tags = new JSONObject();
        tags.put("host", "foo_host");

        datapoint.put(timestamp);
        datapoint.put(histogram.getJson());
        datapoints.put(datapoint);

        metric.put("name", metricName)
                .put("ttl", 600)
                .put("type", "histogram")
                .put("datapoints", datapoints)
                .put("tags", tags);

        json.put(metric);
        final HttpPost post = new HttpPost(KairosHelper.getEndpoint() + "/api/v1/datapoints");
        try {
            post.setEntity(new StringEntity(json.toString()));
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return post;
    }

    private static JSONObject queryJsonFor(final long startTimestamp, final long endTimestamp, final String metric) throws JSONException {
        final JSONObject metricQueryObject = new JSONObject();
        metricQueryObject.put("name", metric);
        final JSONArray metricsList = new JSONArray();
        metricsList.put(metricQueryObject);
        final JSONObject query = new JSONObject();
        query.put("start_absolute", startTimestamp)
                .put("end_absolute", endTimestamp)
                .put("metrics", metricsList);
        return query;
    }

    /**
     * Creates the JSON body for a datapoint query.
     *
     * @param startTimestamp starting timestamp
     * @param endTimestamp ending timestamp
     * @param metric metric to query for
     * @return JSON Object to POST to KairosDB
     * @throws JSONException on JSON error
     */
    public static HttpPost queryFor(final long startTimestamp, final long endTimestamp, final String metric) throws JSONException {
        final JSONObject query = queryJsonFor(startTimestamp, endTimestamp, metric);

        final HttpPost lookup = new HttpPost(KairosHelper.getEndpoint() + "/api/v1/datapoints/query");
        try {
            lookup.setEntity(new StringEntity(query.toString()));
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return lookup;
    }

    /**
     * Creates the JSON body for a datapoint query.
     *
     * @param startTimestamp starting timestamp
     * @param endTimestamp ending timestamp
     * @param metric metric to query for
     * @param aggregator aggregator to apply
     * @param aggregatorParameters extra parameters to apply to the aggregator
     * @return JSON Object to POST to KairosDB
     * @throws JSONException on JSON error
     */
    public static HttpPost queryFor(
            final long startTimestamp,
            final long endTimestamp,
            final String metric,
            final String aggregator,
            final JSONObject aggregatorParameters)
            throws JSONException {
        final JSONObject aggregatorJson = new JSONObject();
        aggregatorJson.put("name", aggregator);

        if (aggregatorParameters != null) {
            final JSONArray names = aggregatorParameters.names();
            if (names != null) {
                for (int i = 0; i < names.length(); i++) {
                    aggregatorJson.putOnce((String) names.get(i), aggregatorParameters.get((String) names.get(i)));
                }
            }
        }

        final JSONArray aggregators = new JSONArray();
        aggregators.put(aggregatorJson);
        final JSONObject query = queryJsonFor(startTimestamp, endTimestamp, metric);
        query.getJSONArray("metrics").getJSONObject(0).put("aggregators", aggregators);

        final HttpPost lookup = new HttpPost(KairosHelper.getEndpoint() + "/api/v1/datapoints/query");
        try {
            lookup.setEntity(new StringEntity(query.toString()));
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return lookup;
    }

    /**
     * Gets the default endpoint for KairosDB running in docker.
     *
     * @return the endpoint string
     */
    private static String getEndpoint() {

        final String dockerHostAddress = System.getProperty("dockerHostAddress");
        final String hostAddress = dockerHostAddress == null ? "localhost" : dockerHostAddress;
        return "http://" + hostAddress + ":8080";
    }

    /**
     * Creates the JSON body for a datapoint POST.
     *
     * @param timestamp timestamp of the datapoint
     * @param number the number
     * @param metricName the name of the metric
     * @return JSON Array to POST to KairosDB
     * @throws JSONException on a JSON error
     */
    public static HttpPost postNumber(final int timestamp, final Double number, final String metricName) throws JSONException {
        final JSONArray json = new JSONArray();
        final JSONObject metric = new JSONObject();
        final JSONArray datapoints = new JSONArray();
        final JSONArray datapoint = new JSONArray();
        final JSONObject tags = new JSONObject();
        tags.put("host", "foo_host");

        datapoint.put(timestamp);
        datapoint.put(number);
        datapoints.put(datapoint);

        metric.put("name", metricName)
                .put("ttl", 600)
                .put("datapoints", datapoints)
                .put("tags", tags);

        json.put(metric);
        final HttpPost post = new HttpPost(KairosHelper.getEndpoint() + "/api/v1/datapoints");
        try {
            post.setEntity(new StringEntity(json.toString()));
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return post;
    }
}
