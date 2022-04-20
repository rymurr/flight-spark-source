package org.apache.arrow.flight.spark;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private final Broadcast<FlightClientOptions> clientOptions;

    public FlightPartitionReaderFactory(Broadcast<FlightClientOptions> clientOptions) {
        this.clientOptions = clientOptions;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition iPartition) {
        // This feels wrong but this is what upstream spark sources do to.
        FlightPartition partition = (FlightPartition) iPartition;
        return new FlightPartitionReader(clientOptions.getValue(), partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition iPartition) {
        // This feels wrong but this is what upstream spark sources do to.
        FlightPartition partition = (FlightPartition) iPartition;
        return new FlightColumnarPartitionReader(clientOptions.getValue(), partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
    
}
