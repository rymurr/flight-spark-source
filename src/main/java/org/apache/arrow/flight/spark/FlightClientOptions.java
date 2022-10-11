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

import java.io.Serializable;
import java.util.List;

public class FlightClientOptions implements Serializable {
    private final String username;
    private final String password;
    private final String trustedCertificates;
    private final String clientCertificate;
    private final String clientKey;
    private final List<FlightClientMiddlewareFactory> middleware;

    public FlightClientOptions(String username, String password, String trustedCertificates, String clientCertificate, String clientKey, List<FlightClientMiddlewareFactory> middleware) {
        this.username = username;
        this.password = password;
        this.trustedCertificates = trustedCertificates;
        this.clientCertificate = clientCertificate;
        this.clientKey = clientKey;
        this.middleware = middleware;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTrustedCertificates() {
        return trustedCertificates;
    }

    public String getClientCertificate() {
        return clientCertificate;
    }

    public String getClientKey() {
        return clientKey;
    }

    public List<FlightClientMiddlewareFactory> getMiddleware() {
        return middleware;
    }
}
