package org.apache.arrow.flight.spark;

import java.io.Serializable;

public class FlightClientOptions implements Serializable {
    private final String trustedCertificates;

    public FlightClientOptions(String trustedCertificates) {
        this.trustedCertificates = trustedCertificates;
    }

    public String getTrustedCertificates() {
        return trustedCertificates;
    }
}
