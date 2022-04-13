package org.apache.arrow.flight.spark;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.spark.sql.connector.read.InputPartition;

public class FlightPartition implements InputPartition {
    private final FlightEndpoint endpoint;

    public FlightPartition(FlightEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String[] preferredLocations() {
        return endpoint.getLocations().stream().map(location -> location.getUri().getHost()).toArray(String[]::new);
    }

    public FlightEndpoint getEndpoint() {
        return endpoint;
    }
}
