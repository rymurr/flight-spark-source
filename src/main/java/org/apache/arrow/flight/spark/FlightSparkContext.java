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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FlightSparkContext {

  private SparkConf conf;

  private final DataFrameReader reader;

  public FlightSparkContext(SparkSession spark) {
    this.conf = spark.sparkContext().getConf();
    reader = spark.read().format("org.apache.arrow.flight.spark");
  }

  public Dataset<Row> read(String s) {
    return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
      .option("uri", String.format(
        "grpc://%s:%s",
        conf.get("spark.flight.endpoint.host"),
        conf.get("spark.flight.endpoint.port")))
      .option("username", conf.get("spark.flight.auth.username"))
      .option("password", conf.get("spark.flight.auth.password"))
      .load(s);
  }

  public Dataset<Row> readSql(String s) {
    return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
      .option("uri", String.format(
        "grpc://%s:%s",
        conf.get("spark.flight.endpoint.host"),
        conf.get("spark.flight.endpoint.port")))
      .option("username", conf.get("spark.flight.auth.username"))
      .option("password", conf.get("spark.flight.auth.password"))
      .load(s);
  }
}
