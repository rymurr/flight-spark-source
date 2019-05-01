package com.dremio.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class DremioDataSourceReader implements DataSourceReader {
    private DataSourceOptions dataSourceOptions;

    public DremioDataSourceReader(DataSourceOptions dataSourceOptions, BufferAllocator allocator) {
        this.dataSourceOptions = dataSourceOptions;
        FlightClient c = new FlightClient(allocator, new Location(dataSourceOptions.get("host").orElse("localhost"), dataSourceOptions.getInt("port", 43430)));
        c.authenticateBasic(dataSourceOptions.get("username").orElse("anonymous"), dataSourceOptions.get("password").orElse(""));
        FlightInfo info = c.getInfo(FlightDescriptor.path("sys", "options"));

//        FlightStream s = c.getStream(info.getEndpoints().get(0).getTicket());
    }

    public StructType readSchema() {
        return null;
    }

    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        return null;
    }
}
