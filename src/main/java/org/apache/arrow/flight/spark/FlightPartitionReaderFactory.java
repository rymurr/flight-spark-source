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

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private final Broadcast<FlightClientOptions> clientOptions;

    public FlightPartitionReaderFactory(Broadcast<FlightClientOptions> clientOptions) {
        this.clientOptions = clientOptions;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition iPartition) {
        // This feels wrong but this is what upstream spark sources do to.
        FlightPartition partition = (FlightPartition) iPartition;
        return new FlightPartitionReader(clientOptions.getValue(), partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition iPartition) {
        // This feels wrong but this is what upstream spark sources do to.
        FlightPartition partition = (FlightPartition) iPartition;
        return new FlightColumnarPartitionReader(clientOptions.getValue(), partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
    
}
