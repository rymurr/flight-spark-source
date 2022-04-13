package org.apache.arrow.flight.spark;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

public class FlightScan implements Scan, Batch {
    private final StructType schema;
    private final FlightClientFactory clientFactory;
    private final FlightInfo info;
    public FlightScan(StructType schema, FlightClientFactory clientFactory, FlightInfo info) {
        this.schema = schema;
        this.clientFactory = clientFactory;
        this.info = info;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] batches = info.getEndpoints().stream().map(endpoint -> {
            return new FlightPartition(endpoint);
        }).toArray(InputPartition[]::new);
        return batches;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new FlightPartitionReaderFactory(clientFactory);
    }
    
}
