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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class FlightClientFactory implements AutoCloseable {
  private final BufferAllocator allocator = new RootAllocator();
  private final Location defaultLocation;
  private final FlightClientOptions clientOptions;

  private CredentialCallOption callOption;

  public FlightClientFactory(Location defaultLocation, FlightClientOptions clientOptions) {
    this.defaultLocation = defaultLocation;
    this.clientOptions = clientOptions;
  }

  public FlightClient apply() {
    FlightClient.Builder builder = FlightClient.builder(allocator, defaultLocation);

    if (!clientOptions.getTrustedCertificates().isEmpty()) {
      builder.trustedCertificates(new ByteArrayInputStream(clientOptions.getTrustedCertificates().getBytes()));
    }

    String clientCertificate = clientOptions.getClientCertificate();
    if (clientCertificate != null && !clientCertificate.isEmpty()) {
      InputStream clientCert = new ByteArrayInputStream(clientCertificate.getBytes());
      InputStream clientKey = new ByteArrayInputStream(clientOptions.getClientKey().getBytes());
      builder.clientCertificate(clientCert, clientKey);
    }

    // Add client middleware
    clientOptions.getMiddleware().stream().forEach(middleware -> builder.intercept(middleware));

    FlightClient client = builder.build();
    String username = clientOptions.getUsername();
    if (username != null && !username.isEmpty()) {
      this.callOption = client.authenticateBasicToken(clientOptions.getUsername(), clientOptions.getPassword()).get();
    }

    return client;
  }

  public CredentialCallOption getCallOption() {
    return this.callOption;
  }

  @Override
  public void close() {
    allocator.close();
  }
}
