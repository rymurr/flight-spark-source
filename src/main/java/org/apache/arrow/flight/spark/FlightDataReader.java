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

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightDataReader implements InputPartitionReader<ColumnarBatch> {
  private static final Logger logger = LoggerFactory.getLogger(FlightDataReader.class);
  private FlightClient client;
  private FlightStream stream;
  private BufferAllocator allocator = null;
  private FlightClientFactory clientFactory;
  private final Ticket ticket;
  private final Broadcast<FlightDataSourceReader.FactoryOptions> options;
  private final Location location;
  private boolean parallel;

  public FlightDataReader(Broadcast<FlightDataSourceReader.FactoryOptions> options) {
    this.options = options;
    this.location = Location.forGrpcInsecure(options.value().getHost(), options.value().getPort());
    this.ticket = new Ticket(options.value().getTicket());
  }

  private void start() {
    if (allocator != null) {
      return;
    }
    FlightDataSourceReader.FactoryOptions options = this.options.getValue();
    this.parallel = options.isParallel();
    this.allocator = new RootAllocator();
    logger.warn("setting up a data reader at host {} and port {} with ticket {}", options.getHost(), options.getPort(), new String(ticket.getBytes()));
    clientFactory = new FlightClientFactory(location, options.getUsername(), options.getPassword(), parallel);
    client = clientFactory.apply();
    stream = client.getStream(ticket);
    if (parallel) {
      logger.debug("doing create action for ticket {}", new String(ticket.getBytes()));
      client.doAction(new Action("create", ticket.getBytes())).forEachRemaining(Object::toString);
      logger.debug("completed create action for ticket {}", new String(ticket.getBytes()));
    }
  }

  @Override
  public boolean next() throws IOException {
    start();
    try {
      return stream.next();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public ColumnarBatch get() {
    start();
    ColumnarBatch batch = new ColumnarBatch(
      stream.getRoot().getFieldVectors()
        .stream()
        .map(FlightArrowColumnVector::new)
        .toArray(ColumnVector[]::new)
    );
    batch.setNumRows(stream.getRoot().getRowCount());
    return batch;
  }

  @Override
  public void close() throws IOException {
        try {
          if (parallel) {
            client.doAction(new Action("delete", ticket.getBytes())).forEachRemaining(Object::toString);
          }
          AutoCloseables.close(stream, client, clientFactory, allocator);
          allocator.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
  }
}
