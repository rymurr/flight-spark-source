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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

public class FlightClientFactory {
  private BufferAllocator allocator;
  private Location defaultLocation;
  private final String username;
  private final String password;

  public FlightClientFactory(BufferAllocator allocator, Location defaultLocation, String username, String password) {
    this.allocator = allocator;
    this.defaultLocation = defaultLocation;
    this.username = username;
    this.password = password;
  }

  public FlightClient apply() {
    FlightClient client = FlightClient.builder(allocator, defaultLocation).build();
    client.authenticateBasic(username, password);
    return client;

  }

}
