package com.dremio.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.types.StructType;

public class FlightDataReaderFactory implements DataReaderFactory<Row> {

    private FlightEndpoint endpoint;
    private final BufferAllocator allocator;
    private final Location defaultLocation;
    private Schema schema;
    private StructType structType;

    public FlightDataReaderFactory(
            FlightEndpoint endpoint,
            BufferAllocator allocator,
            Location defaultLocation,
            Schema schema,
            StructType structType) {
        this.endpoint = endpoint;
        this.allocator = allocator;
        this.defaultLocation = defaultLocation;
        this.schema = schema;
        this.structType = structType;
    }

    @Override
    public String[] preferredLocations() {
        return endpoint.getLocations().stream().map(Location::getHost).toArray(String[]::new);
    }

    @Override
    public DataReader<Row> createDataReader() {
        return new FlightDataReader(endpoint, allocator, defaultLocation, schema, structType);
    }
}
