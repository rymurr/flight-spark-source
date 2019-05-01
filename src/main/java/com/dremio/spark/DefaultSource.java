package com.dremio.spark;

import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

public class DefaultSource implements DataSourceV2, ReadSupport {
    private final RootAllocator rootAllocator = new RootAllocator();
    public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
        return new DremioDataSourceReader(dataSourceOptions, rootAllocator.newChildAllocator(dataSourceOptions.toString(), 0, rootAllocator.getLimit()));
    }
}
