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
                .option("isSql", false)
                .load(s);
    }

    public Dataset<Row> readSql(String s) {
        return reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
                .option("host", conf.get("spark.flight.endpoint.host"))
                .option("username", conf.get("spark.flight.auth.username"))
                .option("password", conf.get("spark.flight.auth.password"))
                .option("isSql", true)
                .load(s);
    }
}
