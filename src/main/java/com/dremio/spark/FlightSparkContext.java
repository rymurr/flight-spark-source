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
package com.dremio.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class FlightSparkContext {

  private SparkConf conf;
  private final DataFrameReader reader;

  private FlightSparkContext(SparkContext sc, SparkConf conf) {
    SQLContext sqlContext = SQLContext.getOrCreate(sc);
    this.conf = conf;
    reader = sqlContext.read().format("com.dremio.spark");
  }

  public static FlightSparkContext flightContext(JavaSparkContext sc) {
    return new FlightSparkContext(sc.sc(), sc.getConf());
  }

  public Dataset<Row> read(String s) {
    return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
      .option("host", conf.get("spark.flight.endpoint.host"))
      .option("username", conf.get("spark.flight.auth.username"))
      .option("password", conf.get("spark.flight.auth.password"))
      .option("parallel", false)
      .load(s);
  }

  public Dataset<Row> readSql(String s) {
    return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
      .option("host", conf.get("spark.flight.endpoint.host"))
      .option("username", conf.get("spark.flight.auth.username"))
      .option("password", conf.get("spark.flight.auth.password"))
      .option("parallel", false)
      .load(s);
  }

  public Dataset<Row> read(String s, boolean parallel) {
    return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
      .option("host", conf.get("spark.flight.endpoint.host"))
      .option("username", conf.get("spark.flight.auth.username"))
      .option("password", conf.get("spark.flight.auth.password"))
      .option("parallel", parallel)
      .load(s);
  }

  public Dataset<Row> readSql(String s, boolean parallel) {
    return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
      .option("host", conf.get("spark.flight.endpoint.host"))
      .option("username", conf.get("spark.flight.auth.username"))
      .option("password", conf.get("spark.flight.auth.password"))
      .option("parallel", parallel)
      .load(s);
  }
}
