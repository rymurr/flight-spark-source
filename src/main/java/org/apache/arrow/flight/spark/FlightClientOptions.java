package org.apache.arrow.flight.spark;

import java.io.Serializable;

public class FlightClientOptions implements Serializable {
    private final String username;
    private final String password;
    private final String trustedCertificates;
    private final String clientCertificate;
    private final String clientKey;

    public FlightClientOptions(String username, String password, String trustedCertificates, String clientCertificate, String clientKey) {
        this.username = username;
        this.password = password;
        this.trustedCertificates = trustedCertificates;
        this.clientCertificate = clientCertificate;
        this.clientKey = clientKey;
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
}
