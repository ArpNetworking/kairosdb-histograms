/*
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

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Allows peeking on a DataPointGroup's iterator.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class PeekableDataPointGroup implements DataPointGroup {
    private final DataPointGroup wrapped;
    private DataPoint peeked = null;
    private boolean havePeeked = false;

    /**
     * Public constructor.
     *
     * @param wrapped The {@link DataPointGroup} to wrap.
     */
    public PeekableDataPointGroup(final DataPointGroup wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public List<GroupByResult> getGroupByResult() {
        return wrapped.getGroupByResult();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public boolean hasNext() {
        return havePeeked || wrapped.hasNext();
    }

    @Override
    public DataPoint next() {
        if (havePeeked) {
            final DataPoint item = peeked;
            havePeeked = false;
            peeked = null;
            return item;
        }
        return wrapped.next();
    }

    /**
     * Peeks at the next element in the DataPointGroup without consuming it.
     *
     * @return the next element
     */
    public DataPoint peek() {
        if (!havePeeked) {
            peeked = wrapped.next();
            havePeeked = true;
        }

        return peeked;
    }

    @Override
    public void remove() {
        if (havePeeked) {
            throw new IllegalStateException("Can't remove after you've peeked at next");
        }
        wrapped.remove();
    }

    @Override
    public void forEachRemaining(final Consumer<? super DataPoint> action) {
        wrapped.forEachRemaining(action);
    }

    @Override
    public Set<String> getTagNames() {
        return wrapped.getTagNames();
    }

    @Override
    public Set<String> getTagValues(final String tag) {
        return wrapped.getTagValues(tag);
    }
}
