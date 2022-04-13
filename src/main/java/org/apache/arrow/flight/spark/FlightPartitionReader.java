package org.apache.arrow.flight.spark;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import org.apache.arrow.flight.FlightStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private final FlightStream stream;
    private Optional<Iterator<InternalRow>> batch;
    private InternalRow row;

    public FlightPartitionReader(FlightStream stream) {
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

    private Iterator<InternalRow> getNextBatch() {
        ColumnarBatch batch = new ColumnarBatch(
            stream.getRoot().getFieldVectors()
            .stream()
            .map(FlightArrowColumnVector::new)
            .toArray(ColumnVector[]::new)
        );
        batch.setNumRows(stream.getRoot().getRowCount());
        return batch.rowIterator();
    }

    // This is written this way because the Spark interface iterates in a different way.
    // E.g., .next() -> .get() vs. .hasNext() -> .next()
    @Override
    public boolean next() throws IOException {
        try {
            // Try the iterator first then get next batch
            // Not quite rust match expressions...
            return batch.map(currentBatch -> {
                // Are there still rows in this batch?
                if (currentBatch.hasNext()) {
                    row = currentBatch.next();
                    return true;
                // No more rows, get the next batch
                } else {
                    // Is there another batch?
                    if (stream.next()) {
                        // Yes, then fetch it.
                        Iterator<InternalRow> nextBatch = getNextBatch();
                        batch = Optional.of(nextBatch);
                        if (currentBatch.hasNext()) {
                            row = currentBatch.next();
                            return true;
                        // Odd, we got an empty batch
                        } else {
                            return false;
                        }
                    // This partition / stream is complete
                    } else {
                        return false;
                    }
                }
            // Fetch the first batch
            }).orElseGet(() -> {
                 // Is the stream empty?
                 if (stream.next()) {
                    // No, then fetch the first batch
                    Iterator<InternalRow> firstBatch = getNextBatch();
                    batch = Optional.of(firstBatch);
                    if (firstBatch.hasNext()) {
                        row = firstBatch.next();
                        return true;
                    // Odd, we got an empty batch
                    } else {
                        return false;
                    }
                // The stream was empty...
                } else {
                    return false;
                }
            });
        } catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    @Override
    public InternalRow get() {
        return row;
    }
    
}
