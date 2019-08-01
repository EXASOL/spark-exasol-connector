package com.exasol.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

/** Tests for loading data from Exasol query as dataframes using short and long source formats */
class LoadSuite extends FunSuite with BaseDockerSuite with DataFrameSuiteBase {

  test("runs dataframe show action successfully") {
    createDummyTable()

    val df = spark.read
      .format("com.exasol.spark")
      .option("host", container.host)
      .option("port", s"${container.port}")
      .option("query", s"SELECT * FROM $EXA_SCHEMA.$EXA_TABLE")
      .load()

    df.show(10, false)
  }

  test("runs dataframe take action successfully") {
    createDummyTable()

    val df = spark.read
      .format("com.exasol.spark")
      .option("host", container.host)
      .option("port", s"${container.port}")
      .option("query", s"SELECT * FROM $EXA_SCHEMA.$EXA_TABLE")
      .load()

    val res = df.take(2)
    res.foreach(println)
    assert(res.size === 2)
  }

  test("creates dataframe from user query") {
    createDummyTable()

    val df1 = spark.read
      .format("com.exasol.spark")
      .option("host", container.host)
      .option("port", s"${container.port}")
      .option("query", s"SELECT * FROM $EXA_SCHEMA.$EXA_TABLE")
      .load()

    val cnt1 = df1.count
    val cities = df1.collect().map(x => x(2)).toSet
    assert(cities === Set("Berlin", "Paris", "Lisbon"))

    val df2 = spark.read
      .format("exasol")
      .option("host", container.host)
      .option("port", s"${container.port}")
      .option("query", s"SELECT * FROM $EXA_SCHEMA.$EXA_TABLE")
      .load()

    assert(cnt1 == 3)
    assert(df2.count == cnt1)

    val schema = df2.schema
    assert(schema.exists(f => f.name == "NAME"))
    assert(
      schema.map(_.name).toSet ===
        Set("ID", "UNICODE_COL", "NAME", "CITY", "DATE_INFO", "UPDATED_AT")
    )

    val typeSet = schema.map(_.dataType).toSet
    assert(typeSet === Set(LongType, StringType, StringType, DateType, TimestampType))
  }

  test("throw exception when query string is not provided") {
    val thrown = intercept[UnsupportedOperationException] {
      spark.read
        .format("com.exasol.spark")
        .option("host", container.host)
        .option("port", s"${container.port}")
        .load()
    }
    assert(
      thrown.getMessage === "A query parameter should be specified in order to run the operation"
    )
  }

  test("return only provided columns from .schema") {
    createDummyTable()

    val expectedSchema = new StructType()
      .add("NAME", StringType)
      .add("UPDATED_AT", TimestampType)

    val df = spark.read
      .format("exasol")
      .option("host", container.host)
      .option("port", s"${container.port}")
      .option("query", s"SELECT * FROM $EXA_SCHEMA.$EXA_TABLE")
      .schema(expectedSchema)
      .load()

    assert(df.schema.length === expectedSchema.length)
    assert(df.schema.map(_.name).toSet === expectedSchema.map(_.name).toSet)
    assert(df.schema.map(_.dataType).toSet === expectedSchema.map(_.dataType).toSet)
    assert(df.collect().map(x => x.getString(0)).toSet === Set("Germany", "France", "Portugal"))
  }

  test("should failed when provided schema in the option is wrong") {
    createDummyTable()

    val expectedSchema = new StructType()
      .add("NAME", StringType)
      .add("UPDATED_AT", TimestampType)
      .add("DATE_INFORMATION", DateType)

    val df = spark.read
      .format("exasol")
      .option("host", container.host)
      .option("port", s"${container.port}")
      .option("query", s"SELECT * FROM $EXA_SCHEMA.$EXA_TABLE")
      .schema(expectedSchema)
      .load()

    // still available for metadata
    assert(df.count() === 3)

    val thrown = intercept[java.sql.SQLException] {
      assert(df.collect().map(x => x.getDate(2)).length === 3)
    }

    assert(
      thrown.getMessage.contains("object \"A\".\"DATE_INFORMATION\" not found")
    )
  }

  test("should use provided sparkConf for loading data from exasol") {
    createDummyTable()

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.exasol.port", s"${container.port}")
      .set("spark.exasol.host", container.host)
      .set("spark.exasol.max_nodes", "200")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = sparkSession.read
      .format("exasol")
      .option("query", s"SELECT CITY FROM $EXA_SCHEMA.$EXA_TABLE")
      .option("port", s"falsePortNumber")
      .option("host", s"falseHostName")
      .load()

    assert(df.count() === 3)
    assert(df.collect().map(_(0)).toSet === Set("Berlin", "Lisbon", "Paris"))
  }

  test("load unicode data from exasol") {
    createDummyTable()

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.exasol.port", s"${container.port}")
      .set("spark.exasol.host", container.host)
      .set("spark.exasol.max_nodes", "200")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = sparkSession.read
      .format("exasol")
      .option(
        "query",
        s"SELECT UNICODE_COL FROM $EXA_SCHEMA.$EXA_TABLE " +
          s" WHERE UNICODE_COL IS NOT NULL"
      )
      .option("port", s"falsePortNumber")
      .option("host", s"falseHostName")
      .load()

    assert(df.count() === 3)
    assert(df.collect().map(_(0)).toSet === Set("öäüß", "Ö", "Ù")) // scalastyle:ignore nonascii
  }

}
