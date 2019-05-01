package com.dremio.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

public class FlightSparkContext {

    private final SQLContext sqlContext;
    private SparkConf conf;
    private final DataFrameReader reader;

    private FlightSparkContext(SparkContext sc, SparkConf conf) {
        sqlContext = SQLContext.getOrCreate(sc);
        this.conf = conf;
        reader = sqlContext.read().format("com.dremio.spark");
    }

    public static FlightSparkContext flightContext(JavaSparkContext sc) {
        return new FlightSparkContext(sc.sc(), sc.getConf());
    }

    public void read(String s) {
        reader.option("port", Integer.parseInt(conf.get("spark.flight.endpoint.port")))
                .option("host", conf.get("spark.flight.endpoint.host"))
                .option("username", conf.get("spark.flight.username"))
                .option("password", conf.get("spark.flight.password"))
                .load(s);
    }
}
