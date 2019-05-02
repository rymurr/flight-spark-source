package com.dremio.spark;

import com.google.common.collect.Maps;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class FlightDataReader implements DataReader<Row> {
    private final FlightClient client;
    private final FlightStream stream;
    private final int schemaSize;
    private final BufferAllocator allocator;
    private final Schema schema;
    private final StructType structType;
    private int subCount = 0;
    private int currentRow = 0;

    public FlightDataReader(
            FlightEndpoint endpoint,
            BufferAllocator allocator,
            Location defaultLocation,
            Schema schema,
            StructType structType) {
        this.allocator = allocator;
        this.schema = schema;
        this.structType = structType;
        client = new FlightClient(allocator,
                (endpoint.getLocations().isEmpty()) ? defaultLocation : endpoint.getLocations().get(0)); //todo multiple locations
        stream = client.getStream(endpoint.getTicket());
        schemaSize = structType.size();

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
        ArrowRow row = new ArrowRow(structType, schema, stream.getRoot(), currentRow, schemaSize);
        currentRow++;
        return row;
    }

    @Override
    public void close() throws IOException {
        allocator.close();
        try {
            client.close();
            stream.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static class ArrowRow implements Row {


        private VectorSchemaRoot root;
        private final int currentRow;
        private final int schemaSize;
        private final StructType schema;


        public ArrowRow(StructType schema,
                        Schema arrowSchema,
                        VectorSchemaRoot root,
                        int currentRow,
                        int schemaSize) {
            this.schema = schema;
            this.currentRow = currentRow;
            this.schemaSize = schemaSize;
            this.root = root;

            IntVector iv = (IntVector) root.getVector("c1");
            int value = iv.get(currentRow);

        }

        @Override
        public int size() {
            return schemaSize;
        }

        @Override
        public int length() {
            return schemaSize;
        }

        @Override
        public StructType schema() {
            return schema;
        }

        @Override
        public Object apply(int i) {
            return get(i);
        }

        @Override
        public Object get(int i) {
            return null;
        }

        @Override
        public boolean isNullAt(int i) {
            return root.getFieldVectors().get(i).isNull(currentRow);
        }

        @Override
        public boolean getBoolean(int i) {
            return ((BitVector) root.getFieldVectors().get(i)).
            return super.getBoolean(i);
        }

        @Override
        public byte getByte(int i) {
            return super.getByte(i);
        }

        @Override
        public short getShort(int i) {
            return super.getShort(i);
        }

        @Override
        public int getInt(int i) {
            return super.getInt(i);
        }

        @Override
        public long getLong(int i) {
            return super.getLong(i);
        }

        @Override
        public float getFloat(int i) {
            return super.getFloat(i);
        }

        @Override
        public double getDouble(int i) {
            return super.getDouble(i);
        }

        @Override
        public String getString(int i) {
            return super.getString(i);
        }

        @Override
        public BigDecimal getDecimal(int i) {
            return super.getDecimal(i);
        }

        @Override
        public Date getDate(int i) {
            return super.getDate(i);
        }

        @Override
        public Timestamp getTimestamp(int i) {
            return super.getTimestamp(i);
        }

        @Override
        public Row copy() {
            return this;
        }

    }
}
