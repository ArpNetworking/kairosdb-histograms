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
package com.arpnetworking.kairosdb.integration;

import com.arpnetworking.kairosdb.Histogram;
import com.arpnetworking.kairosdb.KairosHelper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.kairosdb.testing.AggregatorAndParams;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for storing histogram datapoints.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class AggregationIT {
    private static final List<Histogram> SINGLE_HIST_TEST_DATA = Lists.newArrayList(
            new Histogram(Arrays.asList(1d, 3d, 5d, 7d, 9d, 1d, 9d, 1d, 9d)));
    private static final List<Double> DOUBLE_TEST_DATA = Arrays.asList(1d, 3d, 5d, 7d, 9d, 1d, 9d, 1d, 9d);
    private static final List<Histogram> MULTI_HIST_TEST_DATA = Lists.newArrayList(
            new Histogram(Arrays.asList(9d, 1d, 9d, 1d, 8d, 12d)),
            new Histogram(Arrays.asList(18d, 2d, 18d, 2d, 20d, 20d)));
    private final CloseableHttpClient _client = HttpClients.createDefault();

    // ****  avg aggregator ***
    @Test
    public void testDefaultMeanAggregator() throws IOException, JSONException {
        testDoubleAggregate("avg", DOUBLE_TEST_DATA, 5d);
    }

    @Test
    public void testMeanAggregatorSingle() throws IOException, JSONException {
        testAggregate("avg", SINGLE_HIST_TEST_DATA, 5d);
    }

    @Test
    public void testMeanAggregatorMulti() throws IOException, JSONException {
        testAggregate("avg", MULTI_HIST_TEST_DATA, 10d);
    }

    // ****  count aggregator ***
    @Test
    public void testDefaultCountAggregator() throws IOException, JSONException {
        testDoubleAggregate("count", DOUBLE_TEST_DATA, 9d);
    }

    @Test
    public void testCountAggregatorSingle() throws IOException, JSONException {
        testAggregate("count", SINGLE_HIST_TEST_DATA, 9d);
    }

    @Test
    public void testCountAggregatorMulti() throws IOException, JSONException {
        testAggregate("count", MULTI_HIST_TEST_DATA, 12d);
    }

    // ****  min aggregator ***
    @Test
    public void testDefaultMinAggregator() throws IOException, JSONException {
        testDoubleAggregate("min", DOUBLE_TEST_DATA, 1d);
    }

    @Test
    public void testMinAggregatorSingle() throws IOException, JSONException {
        testAggregate("min", SINGLE_HIST_TEST_DATA, 1d);
    }

    @Test
    public void testMinAggregatorMulti() throws IOException, JSONException {
        testAggregate("min", MULTI_HIST_TEST_DATA, 1d);
    }

    // ****  max aggregator ***
    @Test
    public void testDefaultMaxAggregator() throws IOException, JSONException {
        testDoubleAggregate("max", DOUBLE_TEST_DATA, 9d);
    }

    @Test
    public void testMaxAggregatorSingle() throws IOException, JSONException {
        testAggregate("max", SINGLE_HIST_TEST_DATA, 9d);
    }

    @Test
    public void testMaxAggregatorMulti() throws IOException, JSONException {
        testAggregate("max", MULTI_HIST_TEST_DATA, 20d);
    }

    // ****  sum aggregator ***
    @Test
    public void testDefaultSumAggregator() throws IOException, JSONException {
        testDoubleAggregate("sum", DOUBLE_TEST_DATA, 45d);
    }

    @Test
    public void testSumAggregatorSingle() throws IOException, JSONException {
        testAggregate("sum", SINGLE_HIST_TEST_DATA, 45d);
    }

    @Test
    public void testSumAggregatorMulti() throws IOException, JSONException {
        testAggregate("sum", MULTI_HIST_TEST_DATA, 120d);
    }

    // **** standard deviation aggregator ***
    @Test
    public void testStdDevAggregatorSingle() throws IOException, JSONException {
        testAggregate("dev", SINGLE_HIST_TEST_DATA, Math.sqrt(13d));
    }

    @Test
    public void testStdDevAggregatorMulti() throws IOException, JSONException {
        testAggregate("dev", MULTI_HIST_TEST_DATA, Math.sqrt(628d / 11d));
    }

    @Test
    public void testDefaultStdDevAggregator() throws IOException, JSONException {
        testDoubleAggregate("dev", DOUBLE_TEST_DATA, Math.sqrt(13d));
    }

    // ****  percentile aggregator ***
    @Test
    public void testPercentileRequiresPercentile() throws IOException, JSONException {
        queryWithExpectedCode("metric", 1000, "percentile", samplingParam(), 400);
    }

    @Test
    public void testPercentileRequiresPercentileInRange() throws IOException, JSONException {
        queryWithExpectedCode("metric", 1000, "percentile", percentileParam(-1), 400);
        queryWithExpectedCode("metric", 1000, "percentile", percentileParam(101), 400);
    }

    @Test
    public void testDefaultPercentileAggregator() throws IOException, JSONException {
        testDoubleAggregate("percentile", DOUBLE_TEST_DATA, 1d, percentileParam(0.01));
        testDoubleAggregate("percentile", DOUBLE_TEST_DATA, 5d, percentileParam(0.5));
        testDoubleAggregate("percentile", DOUBLE_TEST_DATA, 9d, percentileParam(0.99));
        testDoubleAggregate("percentile", DOUBLE_TEST_DATA, 9d, percentileParam(1.00));
    }

    @Test
    public void testPercentileAggregatorSingle() throws IOException, JSONException {
        testAggregate("percentile", SINGLE_HIST_TEST_DATA, 1d, percentileParam(0));
        testAggregate("percentile", SINGLE_HIST_TEST_DATA, 5d, percentileParam(0.5));
        testAggregate("percentile", SINGLE_HIST_TEST_DATA, 9d, percentileParam(0.99));
        testAggregate("percentile", SINGLE_HIST_TEST_DATA, 9d, percentileParam(1.00));
    }

    @Test
    public void testPercentileAggregatorMulti() throws IOException, JSONException {
        testAggregate("percentile", MULTI_HIST_TEST_DATA, 1d, percentileParam(0));
        testAggregate("percentile", MULTI_HIST_TEST_DATA, 9d, percentileParam(0.5));
        testAggregate("percentile", MULTI_HIST_TEST_DATA, 20d, percentileParam(0.99));
        testAggregate("percentile", MULTI_HIST_TEST_DATA, 20d, percentileParam(1.00));
    }

    // ****  apdex aggregator ***
    @Test
    public void testApdexRequiresTarget() throws IOException, JSONException {
        queryWithExpectedCode("metric", 1000, "apdex", samplingParam(), 400);
    }

    @Test
    public void testApdexRequiresTargetInRange() throws IOException, JSONException {
        queryWithExpectedCode("metric", 1000, "apdex", apdexParam(-5), 400);
    }

    @Test
    public void testApdexAggregatorSingle() throws IOException, JSONException {
        testAggregate("apdex", SINGLE_HIST_TEST_DATA, 0d, apdexParam(0));
        testAggregate("apdex", SINGLE_HIST_TEST_DATA, 0.3888888888d, apdexParam(1));
        testAggregate("apdex", SINGLE_HIST_TEST_DATA, 0.7777777777d, apdexParam(5));
        testAggregate("apdex", SINGLE_HIST_TEST_DATA, 1d, apdexParam(10));
    }

    @Test
    public void testApdexAggregatorMulti() throws IOException, JSONException {
        testAggregate("apdex", MULTI_HIST_TEST_DATA, 0d, apdexParam(0));
        testAggregate("apdex", MULTI_HIST_TEST_DATA, 0.25d, apdexParam(1));
        testAggregate("apdex", MULTI_HIST_TEST_DATA, 0.666666666d, apdexParam(5));
        testAggregate("apdex", MULTI_HIST_TEST_DATA, 0.791666666d, apdexParam(10));
        testAggregate("apdex", MULTI_HIST_TEST_DATA, 1d, apdexParam(20));
    }

    // ****  merge aggregator ***
    @Test
    public void testMergeAggregatorSingle() throws IOException, JSONException {
        testAggregate("merge", SINGLE_HIST_TEST_DATA, SINGLE_HIST_TEST_DATA.get(0));
    }

    @Test
    public void testMergeAggregatorMulti() throws IOException, JSONException {
        final List<Double> numbers = Lists.newArrayList();
        for (Histogram histogram : MULTI_HIST_TEST_DATA) {
            for (final Map.Entry<Double, Integer> entry : histogram.getBins().entrySet()) {
                for (int count = 0; count < entry.getValue(); count++) {
                    numbers.add(entry.getKey());
                }
            }
        }

        final Histogram merged = new Histogram(numbers);
        testAggregate("merge", MULTI_HIST_TEST_DATA, merged);
    }

    @Test
    public void testAggregateEmptyResults() throws IOException, JSONException {
        final String body = queryWithExpectedCode("non_existing_metric", 1000, "sum", Collections.emptyMap(), 200);
        final JSONObject responseJson = new JSONObject(body);
        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
        Assert.assertEquals(0, queryObject.getInt("sample_size"));
    }

    // **** filter aggregator ***
    @Test
    public void testFilterAggregatorSingle() throws IOException, JSONException {
        final List<Histogram> expected = Lists.newArrayList(
                new Histogram(Arrays.asList(5d, 7d, 9d, 9d, 9d)));
        testAggregate("filter", SINGLE_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testFilterAggregatorMulti() throws IOException, JSONException {
        final List<Histogram> expected = Lists.newArrayList(
                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testFilterAroundZero() throws IOException, JSONException {
        final List<Histogram> input = Lists.newArrayList(new Histogram(Arrays.asList(100d, 0d, -0d, -110d)));
        final List<Histogram> expectedPositiveBins = Lists.newArrayList(new Histogram(Arrays.asList(100d, 0d)));
        testAggregate("filter", input, expectedPositiveBins, filterParam("lte", "keep", +0d));
        final List<Histogram> expectedNegativeBins = Lists.newArrayList(new Histogram(Arrays.asList(-0d, -110d)));
        testAggregate("filter", input, expectedNegativeBins, filterParam("gte", "keep", -0d));
    }

    @Test
    public void testDefaultFilterAggregator() throws IOException, JSONException {
        final List<Double> input = Arrays.asList(9d, 8d, 7d, 6d, 5d, 4d, 3d, 2d, 1d);
        final List<Double> expected = Arrays.asList(9d, 8d, 7d, 6d, 5d);
        testDoubleAggregate("filter", input, expected, filterParam("lt", 5d));
    }

    // **** percent remaining aggregator ***
    @Test
    public void testPercentRemainingAggregatorSolo() throws IOException, JSONException {
        testAggregate("percent_remaining", SINGLE_HIST_TEST_DATA, 1d);
    }

    @Test
    public void testPercentRemainingAggregatorMulti() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregateToDoubles("percent_remaining", MULTI_HIST_TEST_DATA, );
    }

    @Test
    public void testPercentRemainingAggregatorFilterAll() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testPercentRemainingAggregatorFilterNone() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testPercentRemainingAggregatorMergeAll() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testPercentRemainingAggregatorMergeSome() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testPercentRemainingAggregatorFilterThenMerge() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    @Test
    public void testPercentRemainingAggregatorMergeThenFilter() throws IOException, JSONException {
        //TODO
//        final List<Histogram> expected = Lists.newArrayList(
//                new Histogram(Arrays.asList(9d, 9d, 8d, 12d)),
//                new Histogram(Arrays.asList(18d, 18d, 20d, 20d)));
//        testAggregate("filter", MULTI_HIST_TEST_DATA, expected, filterParam("lt", "keep", 5d));
    }

    private Map<String, Object> samplingParam() {
        final Map<String, Object> sampling = Maps.newHashMap();
        sampling.put("value", 10);
        sampling.put("unit", "minutes");
        final Map<String, Object> map = Maps.newHashMap();
        map.put("sampling", sampling);
        return map;
    }

    private Map<String, Object> percentileParam(final double percentile) {
        final Map<String, Object> map = Maps.newHashMap();
        map.put("percentile", percentile);
        final Map<String, Object> sampling = Maps.newHashMap();
        sampling.put("value", 10);
        sampling.put("unit", "minutes");
        map.put("sampling", sampling);
        return map;
    }

    private Map<String, Object> filterParam(final String op, final String inclusion, final double threshold) {
        final Map<String, Object> params = Maps.newHashMap();
        params.put("filter_op", op);
        params.put("filter_indeterminate_inclusion", inclusion);
        params.put("threshold", threshold);
        return params;
    }

    private Map<String, Object> filterParam(final String op, final double threshold) {
        final Map<String, Object> params = Maps.newHashMap();
        params.put("filter_op", op);
        params.put("threshold", threshold);
        return params;
    }

    private Map<String, Object> apdexParam(final double target) {
        final Map<String, Object> map = Maps.newHashMap();
        map.put("target", target);
        final Map<String, Object> sampling = Maps.newHashMap();
        sampling.put("value", 10);
        sampling.put("unit", "minutes");
        map.put("sampling", sampling);
        return map;
    }

    private void testAggregate(final String aggregator, final List<Histogram> histograms, final double expected)
            throws JSONException, IOException {
        testAggregate(aggregator, histograms, expected, samplingParam());
    }

    private void testAggregate(final String aggregator, final List<Histogram> histograms, final Histogram expected)
            throws JSONException, IOException {
        testAggregate(aggregator, histograms, expected, samplingParam());
    }

    private void testDoubleAggregate(final String aggregator, final List<Double> data, final double expected)
            throws JSONException, IOException {
        testDoubleAggregate(aggregator, data, expected, samplingParam());
    }

    private void testDoubleAggregate(
            final String aggregator,
            final List<Double> data,
            final double expected,
            final Map<String, ?> aggParams)
            throws JSONException, IOException {
        final String metricName = newMetricName(aggregator);

        int i = 1;
        for (final Double number : data) {
            postDoubleWithExpectedCode(metricName, i++, number, 204);
        }

        final String body = queryWithExpectedCode(metricName, 1000 + data.size(), aggregator, aggParams, 200);
        verifyQueryResponse(data.size(), expected, body);
    }

    private void testDoubleAggregate(
            final String aggregator,
            final List<Double> data,
            final List<Double> expected,
            final Map<String, ?> aggParams)
            throws JSONException, IOException {
        final String metricName = newMetricName(aggregator);

        int i = 1;
        for (final Double number : data) {
            postDoubleWithExpectedCode(metricName, i++, number, 204);
        }

        final String body = queryWithExpectedCode(metricName, 1000 + data.size(), aggregator, aggParams, 200);
        verifyQueryResponseDouble(data.size(), expected, body);
    }

    private void testAggregate(
            final String aggregator,
            final List<Histogram> histograms,
            final double expected,
            final Map<String, ?> aggParams)
        throws JSONException, IOException {
        final String metricName = newMetricName(aggregator);

        int i = 1;
        for (final Histogram histogram : histograms) {
            postHistogramWithExpectedCode(metricName, i++, histogram, 204);
        }

        final String body = queryWithExpectedCode(metricName, 1000 + histograms.size(), aggregator, aggParams, 200);
        verifyQueryResponse(histograms.size(), expected, body);
    }

    private void testAggregate(
            final String aggregator,
            final List<Histogram> histograms,
            final Histogram expected,
            final Map<String, ?> aggParams)
            throws JSONException, IOException {
        final String metricName = newMetricName(aggregator);

        int i = 1;
        for (final Histogram histogram : histograms) {
            postHistogramWithExpectedCode(metricName, i++, histogram, 204);
        }

        final String body = queryWithExpectedCode(metricName, 1000 + histograms.size(), aggregator, aggParams, 200);
        verifyQueryResponse(histograms.size(), expected, body);
    }

    private void testAggregate(
            final String aggregator,
            final List<Histogram> histograms,
            final List<Histogram> expected,
            final Map<String, ?> aggParams)
            throws JSONException, IOException {
        final String metricName = newMetricName(aggregator);

        int i = 1;
        for (final Histogram histogram : histograms) {
            postHistogramWithExpectedCode(metricName, i++, histogram, 204);
        }

        final String body = queryWithExpectedCode(metricName, 1000 + histograms.size(), aggregator, aggParams, 200);
        verifyQueryResponse(histograms.size(), expected, body);
    }

    //TODO
//    private void testAggregateToDoubles(
//            final List<Histogram> histograms,
//            final List<Double> expected,
//            final AggregatorAndParams... aggregators)
//            throws JSONException, IOException {
//        final String metricName = newMetricName(aggregator);
//
//        int i = 1;
//        for (final Histogram histogram : histograms) {
//            postHistogramWithExpectedCode(metricName, i++, histogram, 204);
//        }
//
//        final String body = queryWithExpectedCode(metricName, 1000 + histograms.size(), aggregator, aggParams, 200);
//        verifyQueryResponseDouble(histograms.size(), expected, body);
//    }

    private void verifyQueryResponse(
            final int expectedSamples,
            final double expectedResult,
            final String responseBody)
            throws JSONException {
        final JSONObject responseJson = new JSONObject(responseBody);
        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
        Assert.assertEquals(expectedSamples, queryObject.getInt("sample_size"));
        final JSONArray result = queryObject.getJSONArray("results").getJSONObject(0).getJSONArray("values").getJSONArray(0);
        Assert.assertEquals(1, result.getInt(0));
        final double avg = result.getDouble(1);
        Assert.assertEquals(expectedResult, avg, 0.000001);
    }

    private void verifyQueryResponse(
            final int expectedSamples,
            final Histogram expectedResult,
            final String responseBody)
            throws JSONException {
        final JSONObject responseJson = new JSONObject(responseBody);
        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
        Assert.assertEquals(expectedSamples, queryObject.getInt("sample_size"));
        final JSONArray result = queryObject.getJSONArray("results").getJSONObject(0).getJSONArray("values").getJSONArray(0);
        Assert.assertEquals(1, result.getInt(0));
        final JSONObject histObject = result.getJSONObject(1);
        final Histogram returnHistogram = new Histogram(histObject);
        Assert.assertEquals(expectedResult, returnHistogram);
    }

    private void verifyQueryResponse(
            final int expectedSamples,
            final List<Histogram> expectedResult,
            final String responseBody)
            throws JSONException {
        final JSONObject responseJson = new JSONObject(responseBody);
        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
        Assert.assertEquals(expectedSamples, queryObject.getInt("sample_size"));
        final JSONArray result = queryObject.getJSONArray("results")
                .getJSONObject(0).getJSONArray("values");

        for (int i = 0; i < result.length(); i++) {
            final JSONArray jsonPair = result.getJSONArray(i);
            Assert.assertEquals(i + 1, jsonPair.getInt(0));
            final Histogram actual = new Histogram(jsonPair.getJSONObject(1));

            Assert.assertEquals(expectedResult.get(i), actual);
        }
    }

    private void verifyQueryResponseDouble(
            final int expectedSamples,
            final List<Double> expectedResult,
            final String responseBody)
            throws JSONException {
        final JSONObject responseJson = new JSONObject(responseBody);
        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
        Assert.assertEquals(expectedSamples, queryObject.getInt("sample_size"));
        final JSONArray result = queryObject.getJSONArray("results")
                .getJSONObject(0).getJSONArray("values");

        Assert.assertEquals(result.length(), expectedResult.size());
        for (int i = 0; i < result.length(); i++) {
            final JSONArray jsonPair = result.getJSONArray(i);
            Assert.assertEquals(i + 1, jsonPair.getInt(0));
            final Double actual = jsonPair.getDouble(1);

            Assert.assertEquals(expectedResult.get(i), actual);
        }
    }

    private void postDoubleWithExpectedCode(
            final String metricName,
            final int timestamp,
            final Double number,
            final int expectedCode) throws JSONException, IOException {
        final HttpPost post = KairosHelper.postNumber(timestamp, number, metricName);
        try (CloseableHttpResponse response = _client.execute(post)) {
            final String body;
            if (response.getEntity() != null) {
                body = CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8));
            } else {
                body = "[empty]";
            }
            Assert.assertEquals("response: " + body, expectedCode, response.getStatusLine().getStatusCode());
        }
    }

    private void postHistogramWithExpectedCode(
            final String metricName,
            final int timestamp,
            final Histogram histogram,
            final int expectedCode) throws JSONException, IOException {
        final HttpPost post = KairosHelper.postHistogram(timestamp, histogram, metricName);
        try (CloseableHttpResponse response = _client.execute(post)) {
            Assert.assertEquals(expectedCode, response.getStatusLine().getStatusCode());
        }
    }

    private static String newMetricName(final String prefix) {
        return prefix + "_test_" + UUID.randomUUID();
    }

    private String queryWithExpectedCode(
            final String metricName,
            final long endTime,
            final String aggregator,
            final Map<String, ?> aggParams,
            final int expectedCode)
            throws JSONException, IOException {
        final JSONObject params = new JSONObject(aggParams);
        final HttpPost queryRequest = KairosHelper.queryFor(1, endTime, metricName, aggregator, params);
        try (CloseableHttpResponse lookupResponse = _client.execute(queryRequest)) {
            final String body = CharStreams.toString(new InputStreamReader(lookupResponse.getEntity().getContent(), Charsets.UTF_8));
            Assert.assertEquals("response: " + body, expectedCode, lookupResponse.getStatusLine().getStatusCode());
            return body;
        }
    }

    //TODO
//    private String queryWithExpectedCode(
//            final long endTime,
//            final String aggregator,
//            final int expectedCode,
//            final AggregatorAndParams... aggregators)
//            throws JSONException, IOException {
//        final JSONObject params = new JSONObject(aggParams);
//        final HttpPost queryRequest = KairosHelper.queryFor(1, endTime, metricName, aggregator, params);
//        try (CloseableHttpResponse lookupResponse = _client.execute(queryRequest)) {
//            final String body = CharStreams.toString(new InputStreamReader(lookupResponse.getEntity().getContent(), Charsets.UTF_8));
//            Assert.assertEquals("response: " + body, expectedCode, lookupResponse.getStatusLine().getStatusCode());
//            return body;
//        }
//    }
}
