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
import org.kairosdb.core.annotation.FeatureComponent;

import javax.inject.Named;

/**
 * Max aggregator that delegates to the built-in and the histogram aggregators.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@FeatureComponent(
        name = "max",
        description = "Returns the maximum value data point for the time range.")
public final class DelegatingMaxAggregator extends DelegatingAggregator {
    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    @Inject
    public DelegatingMaxAggregator(@Named("max") final DelegatingAggregatorMap aggregatorMap) {
        super(aggregatorMap);
    }
}
