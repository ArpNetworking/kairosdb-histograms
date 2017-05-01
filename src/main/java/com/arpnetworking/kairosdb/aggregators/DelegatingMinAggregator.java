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
package com.arpnetworking.kairosdb.aggregators;

import com.arpnetworking.kairosdb.DelegatingAggregatorMap;
import com.google.inject.Inject;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.aggregator.annotation.AggregatorProperty;

import javax.inject.Named;

/**
 * Min aggregator that delegates to the built-in and the histogram aggregators.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@AggregatorName(
        name = "min",
        description = "Returns the minimum value data point for the time range.",
        properties = {
                @AggregatorProperty(name = "sampling", type = "duration"),
                @AggregatorProperty(name = "align_start_time", type = "boolean")
        }
)
public final class DelegatingMinAggregator extends DelegatingAggregator {
    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    @Inject
    public DelegatingMinAggregator(@Named("min") final DelegatingAggregatorMap aggregatorMap) {
        super(aggregatorMap);
    }
}
