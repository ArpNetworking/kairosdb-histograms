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

import com.arpnetworking.kairosdb.aggregators.DelegatingAvgAggregator;
import com.arpnetworking.kairosdb.aggregators.DelegatingCountAggregator;
import com.arpnetworking.kairosdb.aggregators.DelegatingMaxAggregator;
import com.arpnetworking.kairosdb.aggregators.DelegatingMinAggregator;
import com.arpnetworking.kairosdb.aggregators.DelegatingPercentileAggregator;
import com.arpnetworking.kairosdb.aggregators.DelegatingStdDevAggregator;
import com.arpnetworking.kairosdb.aggregators.DelegatingSumAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramApdexAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramCountAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramMaxAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramMeanAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramMergeAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramMinAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramPercentileAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramStdDevAggregator;
import com.arpnetworking.kairosdb.aggregators.HistogramSumAggregator;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.aggregator.AvgAggregator;
import org.kairosdb.core.aggregator.CountAggregator;
import org.kairosdb.core.aggregator.MaxAggregator;
import org.kairosdb.core.aggregator.MinAggregator;
import org.kairosdb.core.aggregator.PercentileAggregator;
import org.kairosdb.core.aggregator.StdAggregator;
import org.kairosdb.core.aggregator.SumAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

/**
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
@SuppressWarnings("unchecked")
public class HistogramModule extends AbstractModule {
    @Override
    protected void configure() {
        LOGGER.info("Binding HistogramModule");
        bind(HistogramDataPointFactory.class).in(Scopes.SINGLETON);

        bind(DelegatingAvgAggregator.class);
        bind(HistogramMeanAggregator.class);

        bind(DelegatingCountAggregator.class);
        bind(HistogramCountAggregator.class);

        bind(DelegatingMinAggregator.class);
        bind(HistogramMinAggregator.class);

        bind(DelegatingMaxAggregator.class);
        bind(HistogramMaxAggregator.class);

        bind(DelegatingSumAggregator.class);
        bind(HistogramSumAggregator.class);

        bind(DelegatingPercentileAggregator.class);
        bind(HistogramPercentileAggregator.class);

        bind(HistogramMergeAggregator.class);
        bind(HistogramApdexAggregator.class);

        bind(DelegatingStdDevAggregator.class);
        bind(HistogramStdDevAggregator.class);
    }

    @Provides
    @Named("avg")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getAvgMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramMeanAggregator> histProvider,
            final Provider<AvgAggregator> avgProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, avgProvider));
    }

    @Provides
    @Named("count")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getCountMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramCountAggregator> histProvider,
            final Provider<CountAggregator> countProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, countProvider));
    }

    @Provides
    @Named("min")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getMinMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramMinAggregator> histProvider,
            final Provider<MinAggregator> minProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, minProvider));
    }

    @Provides
    @Named("max")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getMaxMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramMaxAggregator> histProvider,
            final Provider<MaxAggregator> maxProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, maxProvider));
    }

    @Provides
    @Named("sum")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getSumMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramSumAggregator> histProvider,
            final Provider<SumAggregator> sumProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, sumProvider));
    }

    @Provides
    @Named("percentile")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getPercentileMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramPercentileAggregator> histProvider,
            final Provider<PercentileAggregator> percentileProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, percentileProvider));
    }

    @Provides
    @Named("dev")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getStdDevMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramStdDevAggregator> histProvider,
            final Provider<StdAggregator> stdDevProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, stdDevProvider));
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HistogramModule.class);
}
