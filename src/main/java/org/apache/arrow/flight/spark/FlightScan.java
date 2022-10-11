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

import org.apache.spark.sql.connector.read.Scan;

import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

public class FlightScan implements Scan, Batch {
    private final StructType schema;
    private final FlightInfo info;
    private final Broadcast<FlightClientOptions> clientOptions;

    public FlightScan(StructType schema, FlightInfo info, Broadcast<FlightClientOptions> clientOptions) {
        this.schema = schema;
        this.info = info;
        this.clientOptions = clientOptions;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] batches = info.getEndpoints().stream().map(endpoint -> {
            FlightEndpointWrapper endpointWrapper = new FlightEndpointWrapper(endpoint);
            return new FlightPartition(endpointWrapper);
        }).toArray(InputPartition[]::new);
        return batches;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new FlightPartitionReaderFactory(clientOptions);
    }
    
}
