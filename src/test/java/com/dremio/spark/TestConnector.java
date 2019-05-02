package com.dremio.spark;

import org.apache.spark.SparkConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;

public class TestConnector {
    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static FlightSparkContext csc;

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new SparkConf()
                .setAppName("flightTest")
                .setMaster("local[*]")
//                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.flight.endpoint.host", "localhost")
                .set("spark.flight.endpoint.port", "47470")
                .set("spark.flight.username", "dremio")
                .set("spark.flight.password", "dremio123")
                ;
        sc = new JavaSparkContext(conf);
        csc = FlightSparkContext.flightContext(sc);
    }

    @AfterClass
    public static void tearDown() throws Exception  {
        sc.close();
    }

    @Test
    public void testConnect() {
        csc.read("sys.options");
    }

    @Test
    public void testRead() {
        csc.read("sys.options").show();
    }
}
