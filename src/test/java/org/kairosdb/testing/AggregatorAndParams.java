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
package org.kairosdb.testing;

import java.util.Map;

/**
 * Container class to hold an aggregator and its associated parameters for a query.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public class AggregatorAndParams {
    private String _aggregator;
    private Map<String, ?> _params;

    /**
     * Public constructor.
     *
     * @param aggregator the aggregator type
     * @param params the parameters for the aggregator
     */
    public AggregatorAndParams(final String aggregator, final Map<String, ?> params) {
        this._aggregator = aggregator;
        this._params = params;
    }

    public final String getAggregator() {
        return _aggregator;
    }

    public final Map<String, ?> getParams() {
        return _params;
    }
}
