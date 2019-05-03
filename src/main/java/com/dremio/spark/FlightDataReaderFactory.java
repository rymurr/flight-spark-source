package com.dremio.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.types.StructType;

public class FlightDataReaderFactory implements DataReaderFactory<Row> {

    private byte[] ticket;
//    private final BufferAllocator allocator;
    private final String defaultHost;
    private final int defaultPort;
//    private Schema schema;
    private StructType structType;

    public FlightDataReaderFactory(
            byte[] ticket,
//            BufferAllocator allocator,
            String defaultHost,
            int defaultPort,
//            Schema schema,
            StructType structType) {
        this.ticket = ticket;
//        this.allocator = allocator;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
//        this.schema = schema;
        this.structType = structType;
    }

    @Override
    public String[] preferredLocations() {
        return new String[0]; //endpoint.getLocations().stream().map(Location::getHost).toArray(String[]::new);
    }

    @Override
    public DataReader<Row> createDataReader() {
        return new FlightDataReader(ticket, null, defaultHost, defaultPort, structType);
    }
}
