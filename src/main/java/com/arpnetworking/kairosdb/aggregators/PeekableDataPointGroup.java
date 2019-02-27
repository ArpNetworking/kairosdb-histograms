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
    private final DataPointGroup _wrapped;
    private DataPoint _peeked = null;
    private boolean _havePeeked = false;

    /**
     * Public constructor.
     *
     * @param wrapped The {@link DataPointGroup} to wrap.
     */
    public PeekableDataPointGroup(final DataPointGroup wrapped) {
        _wrapped = wrapped;
    }

    @Override
    public String getName() {
        return _wrapped.getName();
    }

    @Override
    public List<GroupByResult> getGroupByResult() {
        return _wrapped.getGroupByResult();
    }

    @Override
    public void close() {
        _wrapped.close();
    }

    @Override
    public boolean hasNext() {
        return _havePeeked || _wrapped.hasNext();
    }

    @Override
    public DataPoint next() {
        if (_havePeeked) {
            final DataPoint item = _peeked;
            _havePeeked = false;
            _peeked = null;
            return item;
        }
        return _wrapped.next();
    }

    /**
     * Peeks at the next element in the DataPointGroup without consuming it.
     *
     * @return the next element
     */
    public DataPoint peek() {
        if (!_havePeeked) {
            _peeked = _wrapped.next();
            _havePeeked = true;
        }

        return _peeked;
    }

    @Override
    public void remove() {
        if (_havePeeked) {
            throw new IllegalStateException("Can't remove after you've peeked at next");
        }
        _wrapped.remove();
    }

    @Override
    public void forEachRemaining(final Consumer<? super DataPoint> action) {
        _wrapped.forEachRemaining(action);
    }

    @Override
    public Set<String> getTagNames() {
        return _wrapped.getTagNames();
    }

    @Override
    public Set<String> getTagValues(final String tag) {
        return _wrapped.getTagValues(tag);
    }
}
