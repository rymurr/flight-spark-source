Spark source for Flight RPC enabled endpoints
=========================================

[<img src="https://img.shields.io/badge/GitHub-rymurr%2Fflight--spark--source-blue.svg?logo=Github">](https://github.com/rymurr/flight-spark-source)
[![Build Status](https://github.com/rymurr/flight-spark-source/actions/workflows/maven-build.yml/badge.svg)](https://github.com/rymurr/flight-spark-source/actions/workflows/maven-build.yml)

This uses the new [Source V2 Interface](https://databricks.com/session/apache-spark-data-source-v2) to connect to 
[Apache Arrow Flight](https://www.dremio.com/understanding-apache-arrow-flight/) endpoints. It is a prototype of what is 
possible with Arrow Flight. The prototype has achieved 50x speed up compared to serial jdbc driver and scales with the
number of Flight endpoints/spark executors being run in parallel.

It currently supports:

* Columnar Batch reading
* Reading in parallel many flight endpoints as Spark partitions 
* filter and project pushdown

It currently lacks:

* support for all Spark/Arrow data types and filters
* write interface to use `DoPut` to write Spark dataframes back to an Arrow Flight endpoint
* leverage the transactional capabilities of the Spark Source V2 interface
* publish benchmark test

## Usage
You can choose to build the JAR locally, or use one of the archived JAR artifacts built from a [Github Actions workflow run](https://github.com/rymurr/flight-spark-source/actions/workflows/maven-build.yml).

1. Take the built JAR file named: `flight-spark-source-1.0-SNAPSHOT-shaded.jar` - and copy it to the spark master node.  For the sake of this example, we will use the `/tmp` directory
2. Ensure you have a Flight server running and accessible to your Spark cluster.  For an example of a Python Flight RPC server - see [this link](https://arrow.apache.org/cookbook/py/flight.html#streaming-parquet-storage-service).   
   NOTE: you will have to add a `get_schema` end-point to that example server for it to work - with signature:   
```def get_schema(self, context, descriptor) -> pyarrow.flight.SchemaResult```   
   See this [link](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html#pyarrow.flight.FlightClient.get_schema) for more details. 
3. On the Spark master - start an interactive Python (or PySpark) session and run something like:
```
import os
from pyspark.sql import SparkSession

# Get a Spark session and load the connector JAR
spark = (SparkSession
         .builder
         .appName("flight client")
         .config("spark.jars", "/tmp/flight-spark-source-1.0-SNAPSHOT-shaded.jar")
         .getOrCreate()
         )

# Read from a Flight RPC server using an arbitrary string containing either a command or path
# Note - this will call the Flight RPC Server's "get_schema" end-point (which must be present to use the connector)
df = (spark.read.format('cdap.org.apache.arrow.flight.spark')
      .option('uri', 'grpc+tls://flight.example.com:8815')
      # -------------------------------------------------------------------
      # Uncomment the following line to trust the server's CA if it self-signed
      #  .option('trustedCertificates', root_ca)  # In this example, root_ca is a str with contents of a PEM-encoded cert
      # -------------------------------------------------------------------
      # Uncomment the following 2 lines to use authentication if your Flight RPC server supports Basic Token auth
      #  .option('username', 'flight_user')
      #  .option('password', os.environ['FLIGHT_PASSWORD'])  # Using an env var containing the password here for better security
      # -------------------------------------------------------------------
      # Uncomment the following 2 lines to use MTLS client certificate verification if your Flight RPC server supports it (MTLS client certs MUST be version 3 or above!!!)
      #  .option('clientCertificate', mtls_cert_chain)  # In this example, mtls_cert_chain is a str with contents of a PEM-encoded client cert (signed by the servers verification CA)
      #  .option('clientKey', mtls_private_key)  # In this example, mtls_private_key is a str with content of a PEM-encoded client private key
      # -------------------------------------------------------------------
      .load('/some_path_or_command')  # A Path or Command supported by the Flight RPC server  
      )

# Pull the data from the Flight RPC Server's end-point(s) to the Spark worker(s)
df.count()
# or
df.show(n=10)
```

## How to build locally
To build from source locally:
1. Clone the repo
2. Make sure you have Java 11 and Maven installed.
3. Run these steps:
```shell
cd flight-spark-source
./build_jar.sh
```

The target JAR will be present in sub-directory: `target` - with filename: `flight-spark-source-1.0-SNAPSHOT-shaded.jar`.
