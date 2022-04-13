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
    public FlightScan(StructType schema, FlightInfo info) {
        this.schema = schema;
        this.info = info;
    }

    @Override
    public StructType readSchema() {
        return schema;
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
        // TODO Auto-generated method stub
        return null;
    }
    
}
