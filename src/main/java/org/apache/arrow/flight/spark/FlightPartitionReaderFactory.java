package org.apache.arrow.flight.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private static final Logger logger = LoggerFactory.getLogger(FlightPartitionReaderFactory.class);
    private final FlightClientFactory clientFactory;

    public FlightPartitionReaderFactory(FlightClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    private FlightStream createStream(InputPartition iPartition) {
        // This feels wrong but this is what upstream spark sources do to.
        FlightPartition partition = (FlightPartition) iPartition;
        logger.info("Reading Flight data from locations: {}", (Object) partition.preferredLocations());
        FlightClient client = clientFactory.apply();
        return client.getStream(partition.getEndpoint().getTicket());
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        FlightStream stream = createStream(partition);
        return new FlightPartitionReader(stream);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        FlightStream stream = createStream(partition);
        return new FlightColumnarPartitionReader(stream);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
    
}
