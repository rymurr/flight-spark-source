/*
 * Copyright (C) 2019 Ryan Murray
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

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightDataReader implements InputPartitionReader<ColumnarBatch> {
  private final Logger logger = LoggerFactory.getLogger(FlightDataReader.class);
  private final FlightClient client;
  private final FlightStream stream;
  private final BufferAllocator allocator;

  public FlightDataReader(
    byte[] ticket,
    String defaultHost,
    int defaultPort, String username, String password, boolean parallel) {
    this.allocator = new RootAllocator();
    logger.warn("setting up a data reader at host {} and port {} with ticket {}", defaultHost, defaultPort, new String(ticket));
    client = FlightClient.builder(this.allocator, Location.forGrpcInsecure(defaultHost, defaultPort)).build(); //todo multiple locations & ssl
    client.authenticateBasic(username, password);
    if (parallel) {
      Iterator<Result> res = client.doAction(new Action("PARALLEL"));
      res.forEachRemaining(Object::toString);
    }
    stream = client.getStream(new Ticket(ticket));
  }

  @Override
  public boolean next() throws IOException {
    return stream.next();
  }

  @Override
  public ColumnarBatch get() {
    ColumnarBatch batch = new ColumnarBatch(
      stream.getRoot().getFieldVectors()
        .stream()
        .map(ModernArrowColumnVector::new)
        .toArray(ColumnVector[]::new)
    );
    batch.setNumRows(stream.getRoot().getRowCount());
    return batch;
  }

  @Override
  public void close() throws IOException {
        try {
            client.close();
            stream.close();
//            allocator.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
  }
}
