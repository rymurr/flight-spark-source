/*
 * Copyright (C) 2019 The flight-spark-source Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.spark;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConversions;

import com.google.common.collect.Lists;
import com.google.common.base.Joiner;

public class FlightScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);
    private static final Joiner WHERE_JOINER = Joiner.on(" and ");
    private static final Joiner PROJ_JOINER = Joiner.on(", ");
    private SchemaResult flightSchema;
    private StructType schema;
    private final Location location;
    private final Broadcast<FlightClientOptions> clientOptions;
    private FlightDescriptor descriptor;
    private String sql;
    private Filter[] pushed;

    public FlightScanBuilder(Location location, Broadcast<FlightClientOptions> clientOptions, String sql) {
        this.location = location;
        this.clientOptions = clientOptions;
        this.sql = sql;
        descriptor = getDescriptor(sql);
    }

    private class Client implements AutoCloseable {
        private final FlightClientFactory clientFactory;
        private final FlightClient client;
        private final CredentialCallOption callOption;

        public Client(Location location, FlightClientOptions clientOptions) {
            this.clientFactory = new FlightClientFactory(location, clientOptions);
            this.client = clientFactory.apply();
            this.callOption = clientFactory.getCallOption();
        }

        public FlightClient get() {
            return client;
        }

        public CredentialCallOption getCallOption() {
          return this.callOption;
        }

        @Override
        public void close() throws Exception {
            AutoCloseables.close(client, clientFactory);
        }
    }

    private void getFlightSchema(FlightDescriptor descriptor) {
        try (Client client = new Client(location, clientOptions.getValue())) {
            LOGGER.info("getSchema() descriptor: %s", descriptor);
            flightSchema = client.get().getSchema(descriptor, client.getCallOption());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Scan build() {
        try (Client client = new Client(location, clientOptions.getValue())) {
            FlightDescriptor descriptor = FlightDescriptor.command(sql.getBytes());
            LOGGER.info("getInfo() descriptor: %s", descriptor);
            FlightInfo info = client.get().getInfo(descriptor, client.getCallOption());
            return new FlightScan(readSchema(), info, clientOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean canBePushed(Filter filter) {
        if (filter instanceof IsNotNull) {
          return true;
        } else if (filter instanceof EqualTo) {
          return true;
        }
        if (filter instanceof GreaterThan) {
          return true;
        }
        if (filter instanceof GreaterThanOrEqual) {
          return true;
        }
        if (filter instanceof LessThan) {
          return true;
        }
        if (filter instanceof LessThanOrEqual) {
          return true;
        }
        LOGGER.error("Cant push filter of type " + filter.toString());
        return false;
    }

    private String valueToString(Object value) {
        if (value instanceof String) {
          return String.format("'%s'", value);
        }
        return value.toString();
    }

    private String generateWhereClause(List<Filter> pushed) {
        List<String> filterStr = Lists.newArrayList();
        for (Filter filter : pushed) {
            if (filter instanceof IsNotNull) {
            filterStr.add(String.format("isnotnull(\"%s\")", ((IsNotNull) filter).attribute()));
            } else if (filter instanceof EqualTo) {
            filterStr.add(String.format("\"%s\" = %s", ((EqualTo) filter).attribute(), valueToString(((EqualTo) filter).value())));
            } else if (filter instanceof GreaterThan) {
            filterStr.add(String.format("\"%s\" > %s", ((GreaterThan) filter).attribute(), valueToString(((GreaterThan) filter).value())));
            } else if (filter instanceof GreaterThanOrEqual) {
            filterStr.add(String.format("\"%s\" <= %s", ((GreaterThanOrEqual) filter).attribute(), valueToString(((GreaterThanOrEqual) filter).value())));
            } else if (filter instanceof LessThan) {
            filterStr.add(String.format("\"%s\" < %s", ((LessThan) filter).attribute(), valueToString(((LessThan) filter).value())));
            } else if (filter instanceof LessThanOrEqual) {
            filterStr.add(String.format("\"%s\" <= %s", ((LessThanOrEqual) filter).attribute(), valueToString(((LessThanOrEqual) filter).value())));
            }
            //todo fill out rest of Filter types
        }
        return WHERE_JOINER.join(filterStr);
    }

    private FlightDescriptor getDescriptor(String sql) {
        return FlightDescriptor.command(sql.getBytes());
    }

    private void mergeWhereDescriptors(String whereClause) {
        sql = String.format("select * from (%s) as where_merge where %s", sql, whereClause);
        descriptor = getDescriptor(sql);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> notPushed = Lists.newArrayList();
        List<Filter> pushed = Lists.newArrayList();
        for (Filter filter : filters) {
        boolean isPushed = canBePushed(filter);
        if (isPushed) {
            pushed.add(filter);
        } else {
            notPushed.add(filter);
        }
        }
        this.pushed = pushed.toArray(new Filter[0]);
        if (!pushed.isEmpty()) {
            String whereClause = generateWhereClause(pushed);
            mergeWhereDescriptors(whereClause);
            getFlightSchema(descriptor);
        }
        return notPushed.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushed;
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
                throw new UnsupportedOperationException("unknown int type with bitwidth " + bitWidth);
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
                return DataTypes.TimestampType; // note i don't know what this will do!
            case Timestamp:
                return DataTypes.TimestampType;
            case Interval:
                return DataTypes.CalendarIntervalType;
            case NONE:
                return DataTypes.NullType;
            default:
                throw new IllegalStateException("Unexpected value: " + fieldType);
        }
    }

    private StructType readSchemaImpl() {
        if (flightSchema == null) {
            getFlightSchema(descriptor);
        }
        StructField[] fields = flightSchema.getSchema().getFields().stream()
                .map(field -> new StructField(field.getName(),
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

    private void mergeProjDescriptors(String projClause) {
        sql = String.format("select %s from (%s) as proj_merge", projClause, sql);
        descriptor = getDescriptor(sql);
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        if (requiredSchema.toSeq().isEmpty()) {
            return;
        }
        StructType schema = readSchema();
        List<String> fields = Lists.newArrayList();
        List<StructField> fieldsLeft = Lists.newArrayList();
        Map<String, StructField> fieldNames = JavaConversions.<StructField>seqAsJavaList(schema.toSeq()).stream()
                .collect(Collectors.toMap(StructField::name, f -> f));
        for (StructField field : JavaConversions.<StructField>seqAsJavaList(requiredSchema.toSeq())) {
            String name = field.name();
            StructField f = fieldNames.remove(name);
            if (f != null) {
                fields.add(String.format("\"%s\"", name));
                fieldsLeft.add(f);
            }
        }
        if (!fieldNames.isEmpty()) {
            this.schema = new StructType(fieldsLeft.toArray(new StructField[0]));
            mergeProjDescriptors(PROJ_JOINER.join(fields));
            getFlightSchema(descriptor);
        }
    }
}
