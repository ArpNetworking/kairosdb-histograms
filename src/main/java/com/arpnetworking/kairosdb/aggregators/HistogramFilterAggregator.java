/**
 * Copyright 2019 Dropbox Inc.
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

import com.arpnetworking.kairosdb.HistogramDataPoint;
import com.arpnetworking.kairosdb.HistogramDataPointFactory;
import com.arpnetworking.kairosdb.HistogramDataPointImpl;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.AggregatedDataPointGroupWrapper;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Aggregator that filters away some bins of a histogram based on an operation and threshold.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
@FeatureComponent(
        name = "hfilter",
        description = "Filters histograms according to filter operation")
public class HistogramFilterAggregator implements Aggregator {

    private static final int PRECISION = 7;

    /**
     * Whether to keep or discard indeterminate buckets when filtering.
     */
    public enum FilterIndeterminate {
        /**
         * Keep the individual buckets in the data point group if it is indeterminate if they should be filtered by the
         * specific query.
         */
        KEEP {
            boolean shouldDiscard(final boolean thresholdAcceptsLowerBound, final boolean thresholdAcceptsUpperBound) {
                return thresholdAcceptsLowerBound && thresholdAcceptsUpperBound;
            }
        },
        /**
         * Discard the individual buckets in the data point group if it is indeterminate if they should be filtered by
         * the specific query.
         */
        DISCARD {
            boolean shouldDiscard(final boolean thresholdAcceptsLowerBound, final boolean thresholdAcceptsUpperBound) {
                return thresholdAcceptsLowerBound || thresholdAcceptsUpperBound;
            }
        };

        abstract boolean shouldDiscard(boolean thresholdAcceptsLowerBound, boolean thresholdAcceptsUpperBound);
    }

    @FeatureProperty(
            name = "filter_op",
            label = "Filter operation",
            description = "The operation performed for each data point.",
            type = "enum",
            options = {"lte", "lt", "gte", "gt", "equal"},
            default_value = "lt"
    )
    private FilterAggregator.FilterOperation _filterop;

    @FeatureProperty(
            name = "filter_indeterminate_inclusion",
            label = "Filter indeterminate inclusion",
            description = "Whether to keep or discard a histogram bin that straddles the threshold when filtering",
            type = "enum",
            options = {"keep", "discard"},
            default_value = "keep"
    )
    private FilterIndeterminate _filterinc;

    @FeatureProperty(
            label = "Threshold",
            description = "The value the operation is performed on. If the operation is lt, then a null data point "
                    + "is returned if the data point is less than the threshold."
    )
    private double _threshold;


    /**
     * Public Constructor.
     */
    @Inject
    public HistogramFilterAggregator() {
        _threshold = 0.0;
        _filterop = FilterAggregator.FilterOperation.LT;
        _filterinc = FilterIndeterminate.KEEP;
    }

    public void setFilterOp(final FilterAggregator.FilterOperation filterop) {
        _filterop = filterop;
    }

    public void setThreshold(final double threshold) {
        _threshold = threshold;
    }

    public void setFilterIndeterminateInclusion(final FilterIndeterminate inclusion) {
        _filterinc = inclusion;
    }

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        return new HistogramFilterDataPointAggregator(dataPointGroup);
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return HistogramDataPointFactory.GROUP_TYPE;
    }

    private static double truncate(final double val) {
        final long mask = 0xffffe00000000000L;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(val) & mask);
    }

    /**
     * Gives an inclusive bound of the bin that the passed in value will be placed in.
     * If the value is positive, it will be an inclusive upper bound, and if it is negative,
     * it will be an inclusive lower bound.
     *
     * @param val the value whose bucket bound will be calculated
     * @return the inclusive upper or lower bound of the bin
     */
    private static double binInclusiveBound(final double val) {
        long bound = Double.doubleToLongBits(val);
        bound >>= 45;
        bound += 1;
        bound <<= 45;
        bound -= 1;

        return Double.longBitsToDouble(bound);
    }

    private static boolean isNegative(final double value) {
        return Double.doubleToLongBits(value) < 0;
    }

    private class HistogramFilterDataPointAggregator extends AggregatedDataPointGroupWrapper {
        HistogramFilterDataPointAggregator(final DataPointGroup innerDataPointGroup) {
            super(innerDataPointGroup);
        }

        public boolean hasNext() {
            return currentDataPoint != null;
        }

        public DataPoint next() {
            final DataPoint dp = currentDataPoint;
            final long timeStamp = dp.getTimestamp();
            final TreeMap<Double, Integer> filtered = Maps.newTreeMap();
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sum = 0;
            long count = 0;
            int originalCount = 0;

            if (dp instanceof HistogramDataPoint) {
                final HistogramDataPoint hist = (HistogramDataPoint) dp;
                originalCount = hist.getOriginalCount();

                for (final Map.Entry<Double, Integer> entry : hist.getMap().entrySet()) {
                    final double binValue = entry.getKey();
                    final int binCount = entry.getValue();
                    if (!shouldDiscard(binValue)) {
                        filtered.put(binValue, binCount);
                        min = Math.min(min, binValue);
                        max = Math.max(max, binValue);
                        sum += binValue * binCount;
                        count += binCount;
                    }
                }
            }

            final double mean = sum / count;
            if (filtered.size() <= 0) {
                min = Double.NaN;
                max = Double.NaN;
                sum = Double.NaN;
            }
            moveCurrentDataPoint();
            return new HistogramDataPointImpl(timeStamp, PRECISION, filtered, min, max, mean, sum, originalCount);
        }

        private void moveCurrentDataPoint() {
            if (hasNextInternal()) {
                currentDataPoint = nextInternal();
            } else {
                currentDataPoint = null;
            }
        }

        private boolean shouldDiscard(final double value) {
            final double lowerBound;
            final double upperBound;
            if (isNegative(value)) {
                upperBound = truncate(value);
                lowerBound = binInclusiveBound(value);
            } else {
                lowerBound = truncate(value);
                upperBound = binInclusiveBound(value);
            }

            //=================================================================
            /*
             * TODO(Joey Jackson):
             *  DESC. The refactored code below uses the .compare method added to the
             *  FilterOp Enum in the base KairosDB library. The pull request listed below
             *  must be merged before this cleaner version of the code can be used. Until then,
             *  the code below is incompatible with the base KairosDB library, but is compatible
             *  with the fork by ddimensia.
             *  Ref:
             *      https://github.com/kairosdb/kairosdb/pull/555
             *      https://github.com/ddimensia/kairosdb
            if (_filterop == FilterAggregator.FilterOperation.EQUAL) {
                if (_filterinc == FilterIndeterminate.DISCARD) {
                    return _threshold >= lowerBound && _threshold <= upperBound;
                } else if (_filterinc == FilterIndeterminate.KEEP) {
                    return false;
                }
            } else {
                final boolean thresholdAcceptsLowerBound = _filterop.compare(lowerBound, _threshold);
                final boolean thresholdAcceptsUpperBound = _filterop.compare(upperBound, _threshold);
                return _filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
            }
            */
            //=================================================================
            final boolean thresholdAcceptsLowerBound;
            final boolean thresholdAcceptsUpperBound;
            switch (_filterop) {
                case LTE:
                    thresholdAcceptsLowerBound = lowerBound <= _threshold;
                    thresholdAcceptsUpperBound = upperBound <= _threshold;
                    return _filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case LT:
                    thresholdAcceptsLowerBound = lowerBound < _threshold;
                    thresholdAcceptsUpperBound = upperBound < _threshold;
                    return _filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case GTE:
                    thresholdAcceptsLowerBound = lowerBound >= _threshold;
                    thresholdAcceptsUpperBound = upperBound >= _threshold;
                    return _filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case GT:
                    thresholdAcceptsLowerBound = lowerBound > _threshold;
                    thresholdAcceptsUpperBound = upperBound > _threshold;
                    return _filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                default: // EQUAL
                    if (_filterinc == FilterIndeterminate.DISCARD) {
                        return _threshold >= lowerBound && _threshold <= upperBound;
                    } else if (_filterinc == FilterIndeterminate.KEEP) {
                        return false;
                    }
            }
            return false;
            //=================================================================
        }
    }
}
