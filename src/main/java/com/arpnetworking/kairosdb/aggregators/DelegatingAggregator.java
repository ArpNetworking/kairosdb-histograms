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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import javax.inject.Provider;

/**
 * Serves as a base for an aggregator that will delegate and intelligently dispatch aggregation to appropriate classes.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class DelegatingAggregator implements Aggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingAggregator.class);
    private final DelegatingAggregatorMap _aggregatorMap;

    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    public DelegatingAggregator(final DelegatingAggregatorMap aggregatorMap) {
        _aggregatorMap = aggregatorMap;
    }

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        final PeekableDataPointGroup wrapped = new PeekableDataPointGroup(dataPointGroup);

        final String dataType;
        if (wrapped.hasNext()) {
            final DataPoint point = wrapped.peek();
            dataType = point.getDataStoreDataType();
        } else {
            dataType = DoubleDataPointFactoryImpl.DST_DOUBLE;
        }

        final Optional<Aggregator> aggregatorOptional = _aggregatorMap.aggregatorForDataStoreDataType(dataType);
        if (!aggregatorOptional.isPresent()) {
            throw new IllegalArgumentException("Cannot aggregate a " + dataType);
        }

        final Aggregator aggregator = aggregatorOptional.get();
        LOGGER.trace("Delegating to a " + aggregator.getClass().getSimpleName());
        setProperties(aggregator);

        return aggregator.aggregate(wrapped);
    }

    /**
     * Provides a way to set additional properties on the delegated aggregator.
     *
     * @param aggregator the delegated aggregator
     */
    protected void setProperties(final Aggregator aggregator) { }

    @Override
    public boolean canAggregate(final String groupType) {
        return _aggregatorMap.aggregatorForGroupType(groupType).isPresent();
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        final Optional<Provider<? extends Aggregator>> provider = _aggregatorMap.aggregatorForGroupType(groupType);
        if (provider.isPresent()) {
            return provider.get().get().getAggregatedGroupType(groupType);
        }
        throw new IllegalArgumentException("Cannot aggregate a " + groupType);
    }
}
