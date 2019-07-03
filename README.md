# Spark Exasol Connector

[![Build Status][travis-badge]][travis-link]
[![Codecov][codecov-badge]][codecov-link]
[![Maven Central][maven-img-badge]][maven-link]

<p style="border: 1px solid black;padding: 10px; background-color: #FFFFCC;">
<span style="font-size:200%">&#128712;</span> Please note that this is an open
source project which is officially supported by Exasol. For any question, you
can contact our support team or open a Github issue.
</p>

## Overview

This is a connector library that supports an integration between
[Exasol][exasol] and [Apache Spark][spark]. Using this connector, users can
create Spark dataframes from Exasol queries and save Spark dataframes as Exasol
tables.

The implementation is based on Spark [DataSources API][spark-ds-api] and Exasol
[Sub Connections][sol-546].

* [Quick Start](#quick-start)
* [Usage](#usage)
* [Configuration](#configuration)
* [Building and Testing](#building-and-testing)

## Quick Start

Here is short code snippets on how to use the connector in your Spark / Scala
applications.

Reading data from Exasol as Spark dataframe:

```scala
// An Exasol sql syntax query string
val exasolQueryString =
  """
    SELECT SALES_DATE, MARKET_ID, PRICE
    FROM RETAIL.SALES
    WHERE MARKET_ID IN (661, 534, 667)
  """

val df = sparkSession
     .read
     .format("exasol")
     .option("host", "10.0.0.11")
     .option("port", "8563")
     .option("username", "sys")
     .option("password", "exaPass")
     .option("query", exasolQueryString)
     .load()
```

Saving a Spark dataframe as an Exasol table:

```scala
val df = sparkSession
     .write
     .mode("append")
     .option("host", "10.0.0.11")
     .option("port", "8563")
     .option("username", "sys")
     .option("password", "exaPass")
     .option("table", "RETAIL.ADJUSTED_SALES")
     .format("exasol")
     .save()
```

Additionally, you can set the parameter on `SparkConf`:

```scala
// Configure spark session
val sparkConf = new SparkConf()
  .setMaster("local[*]")
  .set("spark.exasol.host", "localhost")
  .set("spark.exasol.port", "8563")
  .set("spark.exasol.username", "sys")
  .set("spark.exasol.password", "exasol")
  .set("spark.exasol.max_nodes", "200")

val sparkSession = SparkSession
  .builder()
  .config(sparkConf)
  .getOrCreate()

val queryStr = "SELECT * FROM MY_SCHEMA.MY_TABLE"

val df = sparkSession
     .read
     .format("exasol")
     .option("query", queryStr)
     .load()
```

Please note that parameter values set on Spark configuration will have higher
priority.

For an example walkthrough please check
[docs/example-walkthrough](docs/example-walkthrough.md).

## Usage

The latest release version ([![Maven Central][maven-reg-badge]][maven-link]) is
compiled against Scala 2.11 and Spark 2.1+.

In order to use the connector in your Java or Scala applications, you can
include it as a dependency to your projects by adding artifact information into
`build.sbt` or `pom.xml` files.

### build.sbt

```scala
resolvers ++= Seq("Exasol Releases" at "https://maven.exasol.com/artifactory/exasol-releases")

libraryDependencies += "com.exasol" % "spark-connector_2.11" % "$LATEST_VERSION"
```

### pom.xml

```xml
<repository>
    <id>maven.exasol.com</id>
    <url>https://maven.exasol.com/artifactory/exasol-releases</url>
</repository>

<dependency>
    <groupId>com.exasol</groupId>
    <artifactId>spark-connector_2.11</artifactId>
    <version>$LATEST_VERSION</version>
</dependency>
```

### Alternative Option

As an alternative, you can provide `--repositories` and `--packages` artifact
coordinates to the **spark-submit**, **spark-shell** or **pyspark** commands.

For example:

```sh
spark-shell \
    --repositories https://maven.exasol.com/artifactory/exasol-releases \
    --packages com.exasol:spark-connector_2.11:$LATEST_VERSION
```

### Deployment

Similarly, you can submit packaged application into the Spark cluster.

Using spark-submit:

```sh
spark-submit \
    --master spark://spark-master-url:7077
    --repositories https://maven.exasol.com/artifactory/exasol-releases \
    --packages com.exasol:spark-connector_2.11:$LATEST_VERSION \
    --class com.myorg.SparkExasolConnectorApp \
    --conf spark.exasol.password=exaTru3P@ss \
    path/to/project/folder/target/scala-2.11/sparkexasolconnectorapp_2.11-5.3.1.jar
```

This deployment example also shows that you can configure the Exasol parameters
at startup using `--conf spark.exasol.keyName=value` syntax.

<strong>Please update the `$LATEST_VERION` accordingly with the latest artifact
version number.</strong>

## Configuration

The following configuration parameters can be provided mainly to facilitate a
connection to Exasol cluster.

| Spark Configuration           | Configuration    | Default       | Description
| :---                          | :---             | :---          | :---
|                               | ``query``        | *<none>*      | A query string to send to Exasol
|                               | ``table``        | *<none>*      | A table name (with schema, e.g. my_schema.my_table) to save dataframe
| ``spark.exasol.host``         | ``host``         | ``localhost`` | A host ip address to the **first** Exasol node (e.g. 10.0.0.11)
| ``spark.exasol.port``         | ``port``         | ``8888``      | A port number to connect to Exasol nodes (e.g.  8563)
| ``spark.exasol.username``     | ``username``     | ``sys``       | An Exasol username for logging in
| ``spark.exasol.password``     | ``password``     | ``exasol``    | An Exasol password for logging in
| ``spark.exasol.max_nodes``    | ``max_nodes``    | ``200``       | The number of data nodes in Exasol cluster
| ``spark.exasol.batch_size``   | ``batch_size``   | ``1000``      | The number of records batched before running execute statement when saving dataframe
| ``spark.exasol.create_table`` | ``create_table`` | ``false``     | A permission to create table if it does not exist in Exasol when saving dataframe

## Building and Testing

Clone the repository,

```bash
git clone https://github.com/exasol/spark-exasol-connector

cd spark-exasol-connector/
```

Compile,

```bash
./sbtx compile
```

Run unit tests,

```bash
./sbtx test
```

To run integration tests, a separate docker network should be created first,

```bash
docker network create -d bridge --subnet 192.168.0.0/24 --gateway 192.168.0.1 dockernet
```

then run,

```bash
./sbtx it:test
```

The integration tests requires [docker][docker],
[exasol/docker-db][exa-docker-db], [testcontainers][testcontainers] and
[spark-testing-base][spark-testing-base].

In order to create a bundled jar,

```bash
./sbtx assembly
```

This creates a jar file under `target/` folder. The jar file can be used with
`spark-submit`, `spark-shell` or `pyspark` commands. For example,

```shell
spark-shell --jars /path/to/spark-exasol-connector-assembly-*.jar
```

## FAQ

* Getting an `Connection was lost and could not be reestablished` error

  For example:

  ```txt
  [error] Caused by: com.exasol.jdbc.ConnectFailed: Connection was lost and could not be reestablished.  (SessionID: 1615669509094853970)
  [error]         at com.exasol.jdbc.AbstractEXAConnection.reconnect(AbstractEXAConnection.java:3505)
  [error]         at com.exasol.jdbc.ServerCommunication.handle(ServerCommunication.java:98)
  [error]         at com.exasol.jdbc.AbstractEXAConnection.communication(AbstractEXAConnection.java:2537)
  [error]         at com.exasol.jdbc.AbstractEXAConnection.communication_resultset(AbstractEXAConnection.java:2257)
  [error]         at com.exasol.jdbc.AbstractEXAStatement.execute(AbstractEXAStatement.java:456)
  [error]         at com.exasol.jdbc.EXAStatement.execute(EXAStatement.java:278)
  [error]         at com.exasol.jdbc.AbstractEXAStatement.executeQuery(AbstractEXAStatement.java:601)
  [error]         at com.exasol.spark.rdd.ExasolRDD.compute(ExasolRDD.scala:125)
  [error]         at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:324)
  [error]         at org.apache.spark.rdd.RDD.iterator(RDD.scala:288)
  ```

  This is one of the known issues. This happens when Spark scheduled parallel
  tasks are less than the number of [sub connections][sol-546]. This can be
  mitigated by submitting Spark application with enough resources so that it can
  start parallel tasks that are more or equal to number of parallel Exasol
  connections.

  Additionally, you can limit the Exasol parallel connections using `max_nodes`
  parameter. However, it is not advised to limit this value in production
  environment.

[travis-badge]: https://travis-ci.org/exasol/spark-exasol-connector.svg?branch=master
[travis-link]: https://travis-ci.org/exasol/spark-exasol-connector
[codecov-badge]: https://codecov.io/gh/exasol/spark-exasol-connector/branch/master/graph/badge.svg
[codecov-link]: https://codecov.io/gh/exasol/spark-exasol-connector
[maven-img-badge]: https://img.shields.io/maven-central/v/com.exasol/spark-connector_2.11.svg
[maven-reg-badge]: https://maven-badges.herokuapp.com/maven-central/com.exasol/spark-connector_2.11/badge.svg
[maven-link]: https://maven-badges.herokuapp.com/maven-central/com.exasol/spark-connector_2.11
[exasol]: https://www.exasol.com/en/
[spark]: https://spark.apache.org/
[spark-ds-api]: https://databricks.com/blog/2015/01/09/spark-sql-data-sources-api-unified-data-access-for-the-spark-platform.html
[docker]: https://www.docker.com/
[exa-docker-db]: https://hub.docker.com/r/exasol/docker-db/
[testcontainers]: https://www.testcontainers.org/
[spark-testing-base]: https://github.com/holdenk/spark-testing-base
[sol-546]: https://www.exasol.com/support/browse/SOL-546
