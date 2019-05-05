package com.dremio.spark;

import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightDataReaderFactory implements InputPartition<ColumnarBatch> {

    private byte[] ticket;
    private final String defaultHost;
    private final int defaultPort;

    public FlightDataReaderFactory(
            byte[] ticket,
            String defaultHost,
            int defaultPort) {
        this.ticket = ticket;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
    }

    @Override
    public String[] preferredLocations() {
        return new String[0]; //endpoint.getLocations().stream().map(Location::getHost).toArray(String[]::new);
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {
        return new FlightDataReader(ticket, defaultHost, defaultPort);
    }

}
