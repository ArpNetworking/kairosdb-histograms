/*
 * Copyright 2016 KairosDB Authors
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
package org.kairosdb.testing;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.AbstractDataPointGroup;
import org.kairosdb.core.datastore.Order;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * DataPoint group for a list of datapoints for use in tests.
 *
 * @author KairosDB Authors
 */
public class ListDataPointGroup extends AbstractDataPointGroup {
    private List<DataPoint> dataPoints = new ArrayList<>();
    private Iterator<DataPoint> iterator;

    /**
     * Constructor that delagates name to AbstractDataPointGroup.
     *
     * @param name Name of data point group
     */
    public ListDataPointGroup(final String name) {
        super(name);
    }

    @Override
    public void close() {
    }

    /**
     * Adds a datapoint to this datapoint group.
     *
     * @param dataPoint DataPoint to add
     */
    public void addDataPoint(final DataPoint dataPoint) {
        dataPoints.add(dataPoint);
    }

    @Override
    public boolean hasNext() {
        if (iterator == null) {
            iterator = dataPoints.iterator();
        }

        return iterator.hasNext();
    }

    @Override
    public DataPoint next() {
        if (iterator == null) {
            iterator = dataPoints.iterator();
        }

        return iterator.next();
    }

    /**
     * In-place sort of the datapoints in this datapoint group by value.
     *
     * @param order Order in witch to sort the datapoints
     */
    public void sort(final Order order) {
        if (order == Order.ASC) {
            Collections.sort(dataPoints, new DataPointComparator());
        } else {
            Collections.sort(dataPoints, Collections.reverseOrder(new DataPointComparator()));
        }

    }

    private static class DataPointComparator implements Comparator<DataPoint>, Serializable {

        private static final long serialVersionUID = -3986265694230498036L;

        @Override
        public int compare(final DataPoint point1, final DataPoint point2) {
            long ret = point1.getTimestamp() - point2.getTimestamp();

            if (ret == 0L) {
                ret = Double.compare(point1.getDoubleValue(), point2.getDoubleValue());
            }

            if (ret == 0L) { // Simple hack to break a tie.
                ret = System.identityHashCode(point1) - System.identityHashCode(point2);
            }

            return ret < 0L ? -1 : 1;
        }
    }
}
