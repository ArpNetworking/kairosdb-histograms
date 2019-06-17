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

import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.aggregator.RangeAggregator;

import java.util.List;
import javax.inject.Provider;

/**
 * Provides mapping of data types and data groups to the aggregators that can aggregate them.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class DelegatingRangeAggregatorMap extends GenericAggregatorMap<RangeAggregator> {

    /**
     * Public constructor.
     *
     * @param dataPointFactory Factory for creating data points.
     * @param aggregators The aggregators to map to.
     */
    public DelegatingRangeAggregatorMap(
            final KairosDataPointFactory dataPointFactory,
            final List<Provider<? extends RangeAggregator>> aggregators) {
        super(dataPointFactory, aggregators);
    }
}
