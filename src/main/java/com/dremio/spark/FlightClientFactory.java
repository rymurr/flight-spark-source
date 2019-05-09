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
        FlightClient client = new FlightClient(allocator, defaultLocation);
        client.authenticateBasic(username, password);
        return client;

    }

}
