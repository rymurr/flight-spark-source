/*
 * Copyright (C) 2019 The flight-spark-source Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.spark;

import org.apache.spark.sql.connector.read.InputPartition;

public class FlightPartition implements InputPartition {
    private final FlightEndpointWrapper endpoint;

    public FlightPartition(FlightEndpointWrapper endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String[] preferredLocations() {
        return endpoint.get().getLocations().stream().map(location -> location.getUri().getHost()).toArray(String[]::new);
    }

    public FlightEndpointWrapper getEndpoint() {
        return endpoint;
    }
}
