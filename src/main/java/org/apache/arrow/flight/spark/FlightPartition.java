package org.apache.arrow.flight.spark;

import org.apache.spark.sql.connector.read.InputPartition;

public class FlightPartition implements InputPartition {
    private final FlightEndpointWrapper endpoint;

    public FlightPartition(FlightEndpointWrapper endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String[] preferredLocations() {
        return endpoint.get().getLocations().stream().map(location -> location.getUri().getHost()).toArray(String[]::new);
    }

    public FlightEndpointWrapper getEndpoint() {
        return endpoint;
    }
}
