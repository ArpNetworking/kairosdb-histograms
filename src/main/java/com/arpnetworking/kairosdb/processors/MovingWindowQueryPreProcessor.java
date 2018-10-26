/**
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
package com.arpnetworking.kairosdb.processors;

import com.arpnetworking.kairosdb.aggregators.MovingWindowAggregator;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.http.rest.json.Query;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.QueryPreProcessor;

/**
 * KairosDB QueryPreProcessor that modifies the start time for queries that contain a
 * movingWindow aggregator.  In the case of a movingWindow aggregator the query needs to fetch a
 * windows width worth of data before the specified start time.  By doing this downstream aggregators
 * will be provided with a complete set of datapoints for the first window width of the requested range.
 *
 * @author Gil Markham (gmarkham at dropbox dot com)
 */
public class MovingWindowQueryPreProcessor implements QueryPreProcessor {
    @Override
    public Query preProcessQuery(final Query query) {
        for (QueryMetric queryMetric: query.getQueryMetrics()) {
            final long originalStartTime = queryMetric.getStartTime();
            for (Aggregator aggregator : queryMetric.getAggregators()) {
                if (aggregator instanceof MovingWindowAggregator) {
                    final MovingWindowAggregator mwAgg = (MovingWindowAggregator) aggregator;
                    final long mwStartTime = mwAgg.calculateEarliestStartTime(originalStartTime);
                    if (mwStartTime < queryMetric.getStartTime()) {
                        queryMetric.setStartTime(mwStartTime);
                    }
                }
            }
        }
        return query;
    }
}
