package org.apache.arrow.flight.spark;

import java.util.Set;

import org.apache.arrow.flight.Location;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class FlightTable implements Table, SupportsRead {
    private static final Set<TableCapability> CAPABILITIES = Set.of(TableCapability.BATCH_READ);
    private final String name;
    private final Location location;
    private final String sql;
    private final Broadcast<FlightClientOptions> clientOptions;
    private StructType schema;

    public FlightTable(String name, Location location, String sql, Broadcast<FlightClientOptions> clientOptions) {
        this.name = name;
        this.location = location;
        this.sql = sql;
        this.clientOptions = clientOptions;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            FlightScanBuilder scanBuilder = new FlightScanBuilder(location, clientOptions, sql);
            schema = scanBuilder.readSchema();
        }
        return schema;
    }

    // TODO - We could probably implement partitioning() but it would require server side support

    @Override
    public Set<TableCapability> capabilities() {
        // We only support reading for now
        return CAPABILITIES;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new FlightScanBuilder(location, clientOptions, sql);
    }
}
