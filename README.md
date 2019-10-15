Spark source for Flight enabled endpoints
=========================================

[![Build Status](https://travis-ci.org/rymurr/flight-spark-source.svg?branch=master)](https://travis-ci.org/rymurr/flight-spark-source)

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
* Strongly tied to [Dremio's flight endpoint](https://github.com/dremio-hub/dremio-flight-connector) and should be abstracted 
to generic Flight sources
* Needs to be updated to support new features in Arrow 0.15.0
* write interface to use `DoPut` to write Spark dataframes back to an Arrow Flight endpoint
* leverage the transactional capabilities of the Spark Source V2 interface
* proper benchmark test
* CI build & tests
