package com.dremio.spark;

import com.dremio.proto.flight.commands.Command;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
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
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;

public class FlightDataSourceReader implements SupportsScanColumnarBatch, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightDataSourceReader.class);
    private static final Joiner WHERE_JOINER = Joiner.on(" and ");
    private static final Joiner PROJ_JOINER = Joiner.on(", ");
    private FlightInfo info;
    private FlightDescriptor descriptor;
    private StructType schema;
    private final LinkedBuffer buffer = LinkedBuffer.allocate();
    private final Location defaultLocation;
    private final FlightClientFactory clientFactory;
    private final boolean parallel;
    private String sql;
    private Filter[] pushed;

    public FlightDataSourceReader(DataSourceOptions dataSourceOptions, BufferAllocator allocator) {
        defaultLocation = new Location(
                dataSourceOptions.get("host").orElse("localhost"),
                dataSourceOptions.getInt("port", 47470)
        );
        clientFactory = new FlightClientFactory(allocator,
                defaultLocation,
                dataSourceOptions.get("username").orElse("anonymous"),
                dataSourceOptions.get("password").orElse(null)
                );
        parallel = dataSourceOptions.getBoolean("parallel", false);
        sql = dataSourceOptions.get("path").orElse("");
        descriptor = getDescriptor(dataSourceOptions.getBoolean("isSql", false), sql);
        try (FlightClient client = clientFactory.apply()) {
            info = client.getInfo(descriptor);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private FlightDescriptor getDescriptor(boolean isSql, String path) {
        String query = (!isSql) ? ("select * from " + path) : path;
        byte[] message = ProtostuffIOUtil.toByteArray(new Command(query , parallel), Command.getSchema(), buffer);
        buffer.clear();
        return FlightDescriptor.command(message);
    }

    private StructType readSchemaImpl() {
        StructField[] fields = info.getSchema().getFields().stream()
                .map(field ->
                    new StructField(field.getName(),
                            sparkFromArrow(field.getFieldType()),
                            field.isNullable(),
                            Metadata.empty()))
                .toArray(StructField[]::new);
        return new StructType(fields);
    }

    public StructType readSchema() {
        if (schema == null) {
            schema = readSchemaImpl();
        }
        return schema;
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
        if (!pushed.isEmpty()) {
            String whereClause = generateWhereClause(pushed);
            mergeWhereDescriptors(whereClause);
            try (FlightClient client = clientFactory.apply()) {
                info = client.getInfo(descriptor);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return notPushed.toArray(new Filter[0]);
    }

    private void mergeWhereDescriptors(String whereClause) {
        sql = String.format("select * from (%s) where %s", sql, whereClause);
        descriptor = getDescriptor(true, sql);
    }

    private void mergeProjDescriptors(String projClause) {
        sql = String.format("select %s from (%s)", projClause, sql);
        descriptor = getDescriptor(true, sql);
    }

    private String generateWhereClause(List<Filter> pushed) {
        List<String> filterStr = Lists.newArrayList();
        for (Filter filter: pushed) {
            if (filter instanceof IsNotNull) {
                filterStr.add(String.format("isnotnull(\"%s\")", ((IsNotNull) filter).attribute()));
            } else if (filter instanceof EqualTo){
                filterStr.add(String.format("\"%s\" = %s", ((EqualTo) filter).attribute(), valueToString(((EqualTo) filter).value())));
            }
        }
        return WHERE_JOINER.join(filterStr);
    }

    private String valueToString(Object value) {
        if (value instanceof String) {
            return String.format("'%s'", value);
        }
        return value.toString();
    }

    private boolean canBePushed(Filter filter) {
        if (filter instanceof IsNotNull) {
            return true;
        } else if (filter instanceof EqualTo){
            return true;
        }
        LOGGER.error("Cant push filter of type " + filter.toString());
        return false;
    }

    @Override
    public Filter[] pushedFilters() {
        return pushed;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        if (requiredSchema.toSeq().isEmpty()) {
            return;
        }
        StructType schema = readSchema();
        List<String> fields = Lists.newArrayList();
        List<StructField> fieldsLeft = Lists.newArrayList();
        Map<String, StructField> fieldNames = JavaConversions.seqAsJavaList(schema.toSeq()).stream().collect(Collectors.toMap(StructField::name, f->f));
        for (StructField field: JavaConversions.seqAsJavaList(requiredSchema.toSeq())) {
            String name = field.name();
            StructField f = fieldNames.remove(name);
            if (f != null) {
                fields.add(String.format("\"%s\"",name));
                fieldsLeft.add(f);
            }
        }
        if (!fieldNames.isEmpty()) {
            this.schema = new StructType(fieldsLeft.toArray(new StructField[0]));
            mergeProjDescriptors(PROJ_JOINER.join(fields));
            try (FlightClient client = clientFactory.apply()) {
                info = client.getInfo(descriptor);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
