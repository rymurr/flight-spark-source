package org.apache.arrow.flight.spark;

import java.io.IOException;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.arrow.flight.FlightStream;
import org.apache.spark.sql.vectorized.ColumnVector;

public class FlightColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
    private final FlightStream stream;

    public FlightColumnarPartitionReader(FlightStream stream) {
        this.stream = stream;
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } catch (Exception e) {
            throw new IOException(e);
        } 
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
}
