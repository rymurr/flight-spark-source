package com.dremio.spark;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

public class FlightDataSourceReader implements DataSourceReader {
    private final FlightInfo info;
    private final Location defaultLocation;
    private BufferAllocator allocator;

    public FlightDataSourceReader(DataSourceOptions dataSourceOptions, BufferAllocator allocator) {
        defaultLocation = new Location(
                dataSourceOptions.get("host").orElse("localhost"),
                dataSourceOptions.getInt("port", 43430)
        );
        this.allocator = allocator;
        FlightClient client = new FlightClient(
                allocator.newChildAllocator("data-source-reader", 0, allocator.getLimit()),
                defaultLocation);
        client.authenticateBasic(dataSourceOptions.get("username").orElse("anonymous"), dataSourceOptions.get("password").orElse(""));
        info = client.getInfo(FlightDescriptor.path("sys", "options"));
        try {
            client.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
//            case FixedSizeBinary:
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

    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        return info.getEndpoints().stream().map(endpoint ->
                new FlightDataReaderFactory(endpoint,
                        allocator.newChildAllocator("data-source-reader", 0, allocator.getLimit()),
                        defaultLocation,
                        info.getSchema(),
                        readSchema())).collect(Collectors.toList());
    }
}
