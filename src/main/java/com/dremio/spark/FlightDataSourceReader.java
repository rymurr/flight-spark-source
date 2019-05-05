package com.dremio.spark;

import com.clearspring.analytics.util.Lists;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.List;
import java.util.stream.Collectors;

public class FlightDataSourceReader implements SupportsScanColumnarBatch, SupportsPushDownFilters {
    private final FlightInfo info;
    private final Location defaultLocation;
    private Filter[] pushed;

    public FlightDataSourceReader(DataSourceOptions dataSourceOptions, BufferAllocator allocator) {
        defaultLocation = new Location(
                dataSourceOptions.get("host").orElse("localhost"),
                dataSourceOptions.getInt("port", 47470)
        );
        FlightClient client = new FlightClient(allocator,defaultLocation);
        client.authenticateBasic(dataSourceOptions.get("username").orElse("anonymous"), dataSourceOptions.get("password").orElse(null));
        info = client.getInfo(getDescriptor(dataSourceOptions));
        try {
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private FlightDescriptor getDescriptor(DataSourceOptions dataSourceOptions) {
        if (dataSourceOptions.getBoolean("isSql", false)) {
            return FlightDescriptor.command(dataSourceOptions.get("path").orElse("").getBytes());
        }
        String path = dataSourceOptions.get("path").orElse("");
        List<String> paths = Lists.newArrayList();
        StringBuilder current = new StringBuilder();
        boolean isQuote = false;
        for (char c: path.toCharArray()) {
            if (isQuote && c != '"') {
                current.append(c);
            } else if (isQuote) {
                if (current.length() > 0) {
                    paths.add(current.toString());
                }
                current = new StringBuilder();
                isQuote = false;
            } else if (c == '"') {
                if (current.length() > 0) {
                    paths.add(current.toString());
                }
                current = new StringBuilder();
                isQuote = true;
            } else if (c == '.'){
                if (current.length() > 0) {
                    paths.add(current.toString());
                }
                current = new StringBuilder();
                isQuote = false;
            } else {
                current.append(c);
            }
        }
        paths.add(current.toString());
        return FlightDescriptor.path(paths);
    }

    public StructType readSchema() {
        StructField[] fields = info.getSchema().getFields().stream()
                .map(field ->
                    new StructField(field.getName(),
                            sparkFromArrow(field.getFieldType()),
                            field.isNullable(),
                            Metadata.empty()))
                .toArray(StructField[]::new);
        return new StructType(fields);
    }

    private DataType sparkFromArrow(FieldType fieldType) {
        switch (fieldType.getType().getTypeID()) {
            case Null:
                return DataTypes.NullType;
            case Struct:
                throw new UnsupportedOperationException("have not implemented Struct type yet");
            case List:
                throw new UnsupportedOperationException("have not implemented List type yet");
            case FixedSizeList:
                throw new UnsupportedOperationException("have not implemented FixedSizeList type yet");
            case Union:
                throw new UnsupportedOperationException("have not implemented Union type yet");
            case Int:
                ArrowType.Int intType = (ArrowType.Int) fieldType.getType();
                int bitWidth = intType.getBitWidth();
                if (bitWidth == 8) {
                    return DataTypes.ByteType;
                } else if (bitWidth == 16) {
                    return DataTypes.ShortType;
                } else if (bitWidth == 32) {
                    return DataTypes.IntegerType;
                } else if (bitWidth == 64) {
                    return DataTypes.LongType;
                }
                throw new UnsupportedOperationException("unknow int type with bitwidth " + bitWidth);
            case FloatingPoint:
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) fieldType.getType();
                FloatingPointPrecision precision = floatType.getPrecision();
                switch (precision) {
                    case HALF:
                    case SINGLE:
                        return DataTypes.FloatType;
                    case DOUBLE:
                        return DataTypes.DoubleType;
                }
            case Utf8:
                return DataTypes.StringType;
            case Binary:
            case FixedSizeBinary:
                return DataTypes.BinaryType;
            case Bool:
                return DataTypes.BooleanType;
            case Decimal:
                throw new UnsupportedOperationException("have not implemented Decimal type yet");
            case Date:
                return DataTypes.DateType;
            case Time:
                return DataTypes.TimestampType; //note i don't know what this will do!
            case Timestamp:
                return DataTypes.TimestampType;
            case Interval:
                return DataTypes.CalendarIntervalType;
            case NONE:
                return DataTypes.NullType;
        }
        throw new IllegalStateException("Unexpected value: " + fieldType);
    }

    @Override
    public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
        return info.getEndpoints().stream().map(endpoint -> {
            Location location = (endpoint.getLocations().isEmpty()) ?
                    new Location(defaultLocation.getHost(), defaultLocation.getPort()) :
                    endpoint.getLocations().get(0);
            return new FlightDataReaderFactory(endpoint.getTicket().getBytes(),
                    location.getHost(),
                    location.getPort());
        }).collect(Collectors.toList());
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> notPushed = Lists.newArrayList();
        List<Filter> pushed = Lists.newArrayList();
        for (Filter filter: filters) {
            boolean isPushed = canBePushed(filter);
            if (isPushed) {
                pushed.add(filter);
            } else  {
                notPushed.add(filter);
            }
        }
        this.pushed = pushed.toArray(new Filter[0]);
        return notPushed.toArray(new Filter[0]);
    }

    private boolean canBePushed(Filter filter) {
        if (filter instanceof IsNotNull) {
            return true;
        } else if (filter instanceof EqualTo){
            return true;
        }
        return false;
    }

    @Override
    public Filter[] pushedFilters() {
        return pushed;
    }
}
