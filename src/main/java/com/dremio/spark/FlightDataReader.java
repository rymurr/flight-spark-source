package com.dremio.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.types.StructType;


import java.io.IOException;

public class FlightDataReader implements DataReader<Row> {
    private final FlightClient client;
    private final FlightStream stream;
//    private final int schemaSize;
    private final BufferAllocator allocator;
//    private final Schema schema;
    private final StructType structType;
    private int subCount = 0;
    private int currentRow = 0;

    public FlightDataReader(
            byte[] ticket,
            BufferAllocator allocator,
            String defaultHost,
            int defaultPort,
//            Schema schema,
            StructType structType) {
        this.allocator = new RootAllocator();
//        this.schema = schema;
        this.structType = structType;
        client = new FlightClient(this.allocator, new Location(defaultHost, defaultPort)); //todo multiple locations
        client.authenticateBasic("dremio", "dremio123");
        stream = client.getStream(new Ticket(ticket));
//        schemaSize = structType.size();

    }

    @Override
    public boolean next() throws IOException {
        if (subCount == currentRow) {
            boolean hasNext = stream.next();
            if (!hasNext) {
                return false;
            }
            subCount = stream.getRoot().getRowCount();
            currentRow = 0;
        }
        return true;
    }

    @Override
    public Row get() {
        Row row = new GenericRowWithSchema(stream.getRoot().getFieldVectors()
                .stream()
                .map(v -> {
                    Object o = v.getObject(currentRow);
                    if (o instanceof Text) {
                        return o.toString();
                    }
                    return o;
                })
                .toArray(Object[]::new),
                structType);
        currentRow++;
        return row;
    }

    @Override
    public void close() throws IOException {

        try {
//            client.close();
//            stream.close();
//            allocator.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
