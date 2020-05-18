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

import org.apache.arrow.flight.Location;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

public class DefaultSource implements DataSourceV2, ReadSupport {

  private SparkSession lazySpark;
  private JavaSparkContext lazySparkContext;

  public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
    Location defaultLocation = Location.forGrpcInsecure(
      dataSourceOptions.get("host").orElse("localhost"),
      dataSourceOptions.getInt("port", 47470)
    );
    String sql = dataSourceOptions.get("path").orElse("");
    FlightDataSourceReader.FactoryOptions options = new FlightDataSourceReader.FactoryOptions(
      defaultLocation,
      sql,
      dataSourceOptions.get("username").orElse("anonymous"),
      dataSourceOptions.get("password").orElse(null),
      dataSourceOptions.getBoolean("parallel", false), null);
    Broadcast<FlightDataSourceReader.FactoryOptions> bOptions = lazySparkContext().broadcast(options);
    return new FlightDataSourceReader(bOptions);
  }

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  private JavaSparkContext lazySparkContext() {
    if (lazySparkContext == null) {
      this.lazySparkContext = new JavaSparkContext(lazySparkSession().sparkContext());
    }
    return lazySparkContext;
  }
}
