package org.apache.arrow.flight.spark;

import org.apache.spark.sql.connector.read.Scan;

import org.apache.arrow.flight.FlightInfo;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

public class FlightScan implements Scan, Batch {
    private final StructType schema;
    private final FlightInfo info;
    private final FlightClientOptions clientOptions;

    public FlightScan(StructType schema, FlightInfo info, FlightClientOptions clientOptions) {
        this.schema = schema;
        this.info = info;
        this.clientOptions = clientOptions;
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
            FlightEndpointWrapper endpointWrapper = new FlightEndpointWrapper(endpoint);
            return new FlightPartition(endpointWrapper);
        }).toArray(InputPartition[]::new);
        return batches;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new FlightPartitionReaderFactory(clientOptions);
    }
    
}
