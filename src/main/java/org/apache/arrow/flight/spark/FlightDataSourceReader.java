/*
 * Copyright (C) 2019 Ryan Murray
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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import scala.collection.JavaConversions;

public class FlightDataSourceReader implements SupportsScanColumnarBatch, SupportsPushDownFilters, SupportsPushDownRequiredColumns, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightDataSourceReader.class);
  private static final Joiner WHERE_JOINER = Joiner.on(" and ");
  private static final Joiner PROJ_JOINER = Joiner.on(", ");
  private SchemaResult info;
  private FlightDescriptor descriptor;
  private StructType schema;
  private final Location defaultLocation;
  private final FlightClientFactory clientFactory;
  private String sql;
  private Filter[] pushed;

  public FlightDataSourceReader(DataSourceOptions dataSourceOptions) {
    defaultLocation = Location.forGrpcInsecure(
      dataSourceOptions.get("host").orElse("localhost"),
      dataSourceOptions.getInt("port", 47470)
    );
    clientFactory = new FlightClientFactory(
      defaultLocation,
      dataSourceOptions.get("username").orElse("anonymous"),
      dataSourceOptions.get("password").orElse(null),
      dataSourceOptions.getBoolean("parallel", false)
    );
    sql = dataSourceOptions.get("path").orElse("");
    descriptor = getDescriptor(sql);
    try (FlightClient client = clientFactory.apply()) {
      info = client.getSchema(descriptor);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private FlightDescriptor getDescriptor(String path) {
    return FlightDescriptor.command(path.getBytes());
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
    System.out.println("planBatchInputPartitions");
    return planBatchInputPartitionsParallel();
  }

  private List<InputPartition<ColumnarBatch>> planBatchInputPartitionsParallel() {

    try (FlightClient client = clientFactory.apply()) {
      FlightInfo info = client.getInfo(FlightDescriptor.command(sql.getBytes()));
      return planBatchInputPartitionsSerial(info);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private List<InputPartition<ColumnarBatch>> planBatchInputPartitionsSerial(FlightInfo info) {
    LOGGER.warn("planning partitions for endpoints {}", Joiner.on(", ").join(info.getEndpoints().stream().map(e -> e.getLocations().get(0).getUri().toString()).collect(Collectors.toList())));
    List<InputPartition<ColumnarBatch>> batches = info.getEndpoints().stream().map(endpoint -> {
      Location location = (endpoint.getLocations().isEmpty()) ?
        Location.forGrpcInsecure(defaultLocation.getUri().getHost(), defaultLocation.getUri().getPort()) :
        endpoint.getLocations().get(0);
      return new FlightDataReaderFactory(endpoint.getTicket().getBytes(),
        location.getUri().getHost(),
        location.getUri().getPort(),
        clientFactory.getUsername(),
        clientFactory.getPassword(),
        clientFactory.isParallel());
    }).collect(Collectors.toList());
    LOGGER.info("Created {} batches from arrow endpoints", batches.size());
    return batches;
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
      try (FlightClient client = clientFactory.apply()) {
        info = client.getSchema(descriptor);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return notPushed.toArray(new Filter[0]);
  }

  private void mergeWhereDescriptors(String whereClause) {
    sql = String.format("select * from (%s) where %s", sql, whereClause);
    descriptor = getDescriptor(sql);
  }

  private void mergeProjDescriptors(String projClause) {
    sql = String.format("select %s from (%s)", projClause, sql);
    descriptor = getDescriptor(sql);
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
    Map<String, StructField> fieldNames = JavaConversions.seqAsJavaList(schema.toSeq()).stream().collect(Collectors.toMap(StructField::name, f -> f));
    for (StructField field : JavaConversions.seqAsJavaList(requiredSchema.toSeq())) {
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
      try (FlightClient client = clientFactory.apply()) {
        info = client.getSchema(descriptor);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    clientFactory.close();
  }
}
