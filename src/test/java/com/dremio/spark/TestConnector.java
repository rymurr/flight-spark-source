package com.dremio.spark;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.service.InitializerRegistry;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;

public class TestConnector extends BaseTestQuery {
    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static FlightSparkContext csc;
    private static InitializerRegistry registry;

    @BeforeClass
    public static void setUp() throws Exception {
        registry = new InitializerRegistry(ExecTest.CLASSPATH_SCAN_RESULT, getBindingProvider());
        registry.start();
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
        registry.close();
        sc.close();
    }

    @Test
    public void testConnect() {
        csc.read("sys.options");
    }
}
