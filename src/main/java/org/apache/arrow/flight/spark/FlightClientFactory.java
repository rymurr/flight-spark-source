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

import java.io.InputStream;
import java.util.Optional;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class FlightClientFactory implements AutoCloseable {
  private final BufferAllocator allocator = new RootAllocator();
  private final Location defaultLocation;
  private final Optional<InputStream> trustedCertificates;

  public FlightClientFactory(Location defaultLocation, Optional<InputStream> trustedCertificates) {
    this.defaultLocation = defaultLocation;
    this.trustedCertificates = trustedCertificates;
  }

  public FlightClient apply() {
    FlightClient.Builder builder = FlightClient.builder(allocator, defaultLocation);
    if (trustedCertificates.isPresent()) {
      builder.trustedCertificates(trustedCertificates.get());
    }
    return builder.build();
  }

  @Override
  public void close() {
    allocator.close();
  }
}
