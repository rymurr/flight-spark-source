package org.apache.arrow.flight.spark;

import java.io.IOException;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;
import org.apache.spark.sql.vectorized.ColumnVector;

public class FlightColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
    private final FlightClientFactory clientFactory;;
    private final FlightClient client;
    private final FlightStream stream;

    public FlightColumnarPartitionReader(FlightClientOptions clientOptions, FlightPartition partition) {
        // TODO - Should we handle multiple locations?
        clientFactory = new FlightClientFactory(partition.getEndpoint().get().getLocations().get(0), clientOptions);
        client = clientFactory.apply();
        stream = client.getStream(partition.getEndpoint().get().getTicket());
    }

    // This is written this way because the Spark interface iterates in a different way.
    // E.g., .next() -> .get() vs. .hasNext() -> .next()
    @Override
    public boolean next() throws IOException {
        try {
            return stream.next();
        } catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ColumnarBatch get() {
        ColumnarBatch batch = new ColumnarBatch(
            stream.getRoot().getFieldVectors()
            .stream()
            .map(FlightArrowColumnVector::new)
            .toArray(ColumnVector[]::new)
        );
        batch.setNumRows(stream.getRoot().getRowCount());
        return batch;
    }

    @Override
    public void close() throws IOException {
        try {
            AutoCloseables.close(stream, client, clientFactory);
        } catch (Exception e) {
            throw new IOException(e);
        } 
    } 
}
