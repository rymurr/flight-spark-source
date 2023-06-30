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

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.util.AutoCloseables;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private final FlightClientFactory clientFactory;;
    private final FlightClient client;
    private final CredentialCallOption callOption;
    private final FlightStream stream;
    private Optional<Iterator<InternalRow>> batch;
    private InternalRow row;

    public FlightPartitionReader(FlightClientOptions clientOptions, FlightPartition partition) {
        // TODO - Should we handle multiple locations?
        clientFactory = new FlightClientFactory(partition.getEndpoint().get().getLocations().get(0), clientOptions);
        client = clientFactory.apply();
        callOption = clientFactory.getCallOption();
        stream = client.getStream(partition.getEndpoint().get().getTicket(), callOption);
    }

    private Iterator<InternalRow> getNextBatch() {
        ColumnarBatch batch = new ColumnarBatch(
            stream.getRoot().getFieldVectors()
            .stream()
            .map(FlightArrowColumnVector::new)
            .toArray(ColumnVector[]::new)
        );
        batch.setNumRows(stream.getRoot().getRowCount());
        return batch.rowIterator();
    }

    // This is written this way because the Spark interface iterates in a different way.
    // E.g., .next() -> .get() vs. .hasNext() -> .next()
    @Override
    public boolean next() throws IOException {
        try {
            // Try the iterator first then get next batch
            // Not quite rust match expressions...
            return batch.map(currentBatch -> {
                // Are there still rows in this batch?
                if (currentBatch.hasNext()) {
                    row = currentBatch.next();
                    return true;
                // No more rows, get the next batch
                } else {
                    // Is there another batch?
                    if (stream.next()) {
                        // Yes, then fetch it.
                        Iterator<InternalRow> nextBatch = getNextBatch();
                        batch = Optional.of(nextBatch);
                        if (currentBatch.hasNext()) {
                            row = currentBatch.next();
                            return true;
                        // Odd, we got an empty batch
                        } else {
                            return false;
                        }
                    // This partition / stream is complete
                    } else {
                        return false;
                    }
                }
            // Fetch the first batch
            }).orElseGet(() -> {
                 // Is the stream empty?
                 if (stream.next()) {
                    // No, then fetch the first batch
                    Iterator<InternalRow> firstBatch = getNextBatch();
                    batch = Optional.of(firstBatch);
                    if (firstBatch.hasNext()) {
                        row = firstBatch.next();
                        return true;
                    // Odd, we got an empty batch
                    } else {
                        return false;
                    }
                // The stream was empty...
                } else {
                    return false;
                }
            });
        } catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    @Override
    public InternalRow get() {
        return row;
    }

    @Override
    public void close() throws IOException {
        try {
            AutoCloseables.close(stream, client, clientFactory);
        } catch (Exception e) {
            throw new IOException(e);
        } 
    }   
}
