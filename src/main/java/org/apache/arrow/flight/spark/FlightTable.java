package org.apache.arrow.flight.spark;

import java.io.InputStream;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.flight.Location;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class FlightTable implements Table, SupportsRead {
    private static final Set<TableCapability> CAPABILITIES = Set.of(TableCapability.BATCH_READ);
    private final String name;
    private final FlightClientFactory clientFactory;
    private final String sql;
    private StructType schema;

    public FlightTable(String name, Location location, String sql, Optional<InputStream> trustedCertificates) {
        this.name = name;
        clientFactory = new FlightClientFactory(location, trustedCertificates);
        this.sql = sql;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            schema = (new FlightScanBuilder(clientFactory, sql)).readSchema();
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
        return new FlightScanBuilder(clientFactory, sql);
    }
}
