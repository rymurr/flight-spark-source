package com.dremio.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;


import java.io.IOException;

public class FlightDataReader implements InputPartitionReader<ColumnarBatch> {
    private final FlightClient client;
    private final FlightStream stream;
    private final BufferAllocator allocator;

    public FlightDataReader(
            byte[] ticket,
            String defaultHost,
            int defaultPort) {
        this.allocator = new RootAllocator();
        client = new FlightClient(this.allocator, new Location(defaultHost, defaultPort)); //todo multiple locations
        client.authenticateBasic("dremio", "dremio123");
        stream = client.getStream(new Ticket(ticket));
    }

    @Override
    public boolean next() throws IOException {
        return stream.next();
    }

    @Override
    public ColumnarBatch get() {
        ColumnarBatch batch = new ColumnarBatch(
                stream.getRoot().getFieldVectors()
                        .stream()
                        .map(ArrowColumnVector::new)
                        .toArray(ColumnVector[]::new)
        );
        batch.setNumRows(stream.getRoot().getRowCount());
        return batch;
    }

    @Override
    public void close() throws IOException {

//        try {
//            client.close();
//            stream.close();
//            allocator.close();
//        } catch (Exception e) {
//            throw new IOException(e);
//        }
    }
}
