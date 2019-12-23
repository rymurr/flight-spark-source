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
package com.dremio.spark;

import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightDataReaderFactory implements InputPartition<ColumnarBatch> {

  private byte[] ticket;
  private final String defaultHost;
  private final int defaultPort;
  private final String username;
  private final String password;

  public FlightDataReaderFactory(
    byte[] ticket,
    String defaultHost,
    int defaultPort, String username, String password) {
    this.ticket = ticket;
    this.defaultHost = defaultHost;
    this.defaultPort = defaultPort;
    this.username = username;
    this.password = password;
  }

  @Override
  public String[] preferredLocations() {
    return new String[]{defaultHost};
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    return new FlightDataReader(ticket, defaultHost, defaultPort, username, password);
  }

}
