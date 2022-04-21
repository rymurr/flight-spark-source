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
