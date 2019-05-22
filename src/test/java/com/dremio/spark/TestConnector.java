package com.dremio.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;

import java.util.Properties;
import java.util.function.Consumer;

public class TestConnector {
    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static FlightSparkContext csc;

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new SparkConf()
                .setAppName("flightTest")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.flight.endpoint.host", "localhost")
                .set("spark.flight.endpoint.port", "47470")
                .set("spark.flight.auth.username", "dremio")
                .set("spark.flight.auth.password", "dremio123")
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
        long count = csc.read("sys.options").count();
        Assert.assertTrue(count > 0);
    }

    @Test
    public void testReadWithQuotes() {
        long count = csc.read("\"sys\".options").count();
        Assert.assertTrue(count > 0);
    }

    @Test
    public void testSql() {
        long count = csc.readSql("select * from \"sys\".options").count();
        Assert.assertTrue(count > 0);
    }

    @Test
    public void testFilter() {
        Dataset<Row> df = csc.readSql("select * from \"sys\".options");
        long count = df.filter(df.col("kind").equalTo("LONG")).count();
        long countOriginal = csc.readSql("select * from \"sys\".options").count();
        Assert.assertTrue(count < countOriginal);
    }

    private static class SizeConsumer implements Consumer<Row> {
        private int length = 0;
        private int width = 0;

        @Override
        public void accept(Row row) {
            length+=1;
            width = row.length();
        }
    }

    @Test
    public void testProject() {
        Dataset<Row> df = csc.readSql("select * from \"sys\".options");
        SizeConsumer c = new SizeConsumer();
        df.select("name", "kind", "type").toLocalIterator().forEachRemaining(c);
        long count = c.width;
        long countOriginal = csc.readSql("select * from \"sys\".options").columns().length;
        Assert.assertTrue(count < countOriginal);
    }

    @Test
    public void testParallel() {
        Dataset<Row> df = csc.readSql("select * from \"sys\".options", true);
        SizeConsumer c = new SizeConsumer();
        SizeConsumer c2 = new SizeConsumer();
        df.select("name", "kind", "type").filter(df.col("kind").equalTo("LONG")).toLocalIterator().forEachRemaining(c);
        long width = c.width;
        long length = c.length;
        csc.readSql("select * from \"sys\".options", true).toLocalIterator().forEachRemaining(c2);
        long widthOriginal = c2.width;
        long lengthOriginal = c2.length;
        Assert.assertTrue(width < widthOriginal);
        Assert.assertTrue(length < lengthOriginal);
    }

    @Ignore
    @Test
    public void testSpeed() {
        long[] jdbcT = new long[16];
        long[] flightT = new long[16];
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "dremio");
        connectionProperties.put("password", "dremio123");
        long jdbcC = 0;
        long flightC = 0;
        for (int i=0;i<4;i++) {
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
