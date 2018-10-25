# [WIP] Spark Exasol Connector

[![Build Status][travis-badge]][travis-link]
[![Codecov][codecov-badge]][codecov-link]
[![Maven Central][maven-badge]][maven-link]

###### Please note that this is an open source project which is *not officially supported* by Exasol. We will try to help you as much as possible, but can't guarantee anything since this is not an official Exasol product.

## Overview

This is a connector library that supports an integration between
[Exasol][exasol] and [Apache Spark][spark]. Using this connector, users can
read/write data from/to Exasol using Spark.

* [Quick Start](#quick-start)
* [Usage](#usage)
* [Building and Testing](#building-and-testing)
* [Configuration](#configuration)

## Quick Start

Here is short quick start on how to use the connector.

Reading data from Exasol,

```scala
// This is Exasol SQL Syntax
val exasolQueryString = "SELECT * FROM MY_SCHEMA.MY_TABLE"

val df = sparkSession
     .read
     .format("exasol")
     .option("host", "localhost")
     .option("port", "8888")
     .option("username", "sys")
     .option("password", "exasol")
     .option("query", exasolQueryString)
     .load()

df.show(10, false)
```

Or using spark configurations: (this will have higher priority)
```scala
// config spark session
val sparkConf = new SparkConf()
  .setMaster("local[*]")
  .set("spark.exasol.host", "localhost")
  .set("spark.exasol.port", "8563")
  .set("spark.exasol.username", "sys")
  .set("spark.exasol.password", "exasol")
  .set("spark.exasol.max_nodes", "200")

val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

// This is Exasol SQL Syntax
val exasolQueryString = "SELECT * FROM MY_SCHEMA.MY_TABLE"

val df = sparkSession
     .read
     .format("exasol")
     .option("query", exasolQueryString)
     .load()

df.show(10, false)
```

For more examples you can check [docs/examples](docs/examples.md).

## Usage

You can include the connector as a dependency in your projects.

Using SBT:

```scala
libraryDependencies += "com.exasol" %% "spark-connector" % "<latest-version>"
```

Using Maven:

```xml
<dependency>
    <groupId>com.exasol</groupId>
    <artifactId>spark-connector_2.11</artifactId>
    <version>latest-version</version>
</dependency>
```

## Building and Testing

Clone the repository,

```bash
git clone https://github.com/EXASOL/spark-exasol-connector

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

## Configuration

The following configuration parameters can be provided mainly to facilitate a
connection to Exasol cluster.

| Spark Configuration        | Configuration | Default       | Description
| :---                       | :---          | :---          | :---
|                            | ``query``     | *<none>*      | A query string to send to Exasol
| ``spark.exasol.host``      | ``host``      | ``localhost`` | A host ip address to the **first** Exasol node (e.g. 10.0.0.11)
| ``spark.exasol.port``      | ``port``      | ``8888``      | A port number to connect to Exasol nodes (e.g.  8563)
| ``spark.exasol.username``  | ``username``  | ``sys``       | An Exasol username for logging in
| ``spark.exasol.password``  | ``password``  | ``exasol``    | An Exasol password for logging in
| ``spark.exasol.max_nodes`` | ``max_nodes`` | ``200``       | The number of data nodes in Exasol cluster

[travis-badge]: https://travis-ci.org/EXASOL/spark-exasol-connector.svg?branch=master
[travis-link]: https://travis-ci.org/EXASOL/spark-exasol-connector
[codecov-badge]: https://codecov.io/gh/EXASOL/spark-exasol-connector/branch/master/graph/badge.svg
[codecov-link]: https://codecov.io/gh/EXASOL/spark-exasol-connector
[maven-badge]: https://img.shields.io/maven-central/v/com.exasol/spark-connector_2.11.svg
[maven-link]: https://maven-badges.herokuapp.com/maven-central/com.exasol/spark-connector_2.11
[exasol]: https://www.exasol.com/en/
[spark]: https://spark.apache.org/
[docker]: https://www.docker.com/
[exa-docker-db]: https://hub.docker.com/r/exasol/docker-db/
[testcontainers]: https://www.testcontainers.org/
[spark-testing-base]: https://github.com/holdenk/spark-testing-base
