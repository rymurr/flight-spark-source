package com.dremio.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;

import java.util.Properties;

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
        long[] jdbcT = new long[16];
        long[] flightT = new long[16];
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "dremio");
        connectionProperties.put("password", "dremio123");
        long jdbcC = 0;
        long flightC = 0;
        for (int i=0;i<2;i++) {
            long now = System.currentTimeMillis();
            Dataset<Row> jdbc = SQLContext.getOrCreate(sc.sc()).read().jdbc("jdbc:dremio:direct=localhost:31010", "\"@dremio\".sdd", connectionProperties);
            jdbcC = jdbc.count();
            long then = System.currentTimeMillis();
            flightC = csc.read("@dremio.sdd").count();
            long andHereWeAre = System.currentTimeMillis();
            jdbcT[i] = then-now;
            flightT[i] = andHereWeAre - then;
        }
        for (int i =0;i<16;i++) {
            System.out.println("Trial " + i + ": Flight took " + flightT[i] + " and jdbc took " + jdbcT[i]);
        }
        System.out.println("Fetched " + jdbcC + " row from jdbc and " + flightC + " from flight");
    }
}
