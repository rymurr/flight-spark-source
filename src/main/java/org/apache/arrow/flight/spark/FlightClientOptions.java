package org.apache.arrow.flight.spark;

public class FlightClientOptions {
    private final String trustedCertificates;

    public FlightClientOptions(String trustedCertificates) {
        this.trustedCertificates = trustedCertificates;
    }

    public String getTrustedCertificates() {
        return trustedCertificates;
    }
}
