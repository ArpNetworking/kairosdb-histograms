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

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * Tests for storing histogram datapoints.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class StorageIT {
    @Test
    public void testStoreDataPoint() throws IOException, JSONException {
        final Histogram histogram = new Histogram(Arrays.asList(1d, 3d, 5d, 7d, 9d, 1d, 9d));
        final int timestamp = 10;

        final String metricName = "foo_histogram";
        final HttpPost request = KairosHelper.postHistogram(timestamp, histogram, metricName);
        final CloseableHttpResponse response = _client.execute(request);
        Assert.assertEquals(204, response.getStatusLine().getStatusCode());

        final HttpPost queryRequest = KairosHelper.queryFor(timestamp, timestamp, metricName);
        final CloseableHttpResponse lookupResponse = _client.execute(queryRequest);

        Assert.assertEquals(200, lookupResponse.getStatusLine().getStatusCode());
        final String body = CharStreams.toString(new InputStreamReader(lookupResponse.getEntity().getContent(), Charsets.UTF_8));
        final JSONObject responseJson = new JSONObject(body);
        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
        Assert.assertEquals(1, queryObject.getInt("sample_size"));
        final JSONArray result = queryObject.getJSONArray("results").getJSONObject(0).getJSONArray("values").getJSONArray(0);
        Assert.assertEquals(timestamp, result.getInt(0));
        final JSONObject histogramJson = result.getJSONObject(1);
        final Histogram returnHistogram = new Histogram(histogramJson);
        Assert.assertEquals(histogram, returnHistogram);
    }

    private final CloseableHttpClient _client = HttpClients.createDefault();
}
