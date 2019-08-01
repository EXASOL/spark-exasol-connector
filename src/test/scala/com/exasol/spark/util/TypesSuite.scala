package com.exasol.spark.util

import org.apache.spark.sql.types._

import com.exasol.spark.util.Types._

import org.scalatest.{FunSuite, Matchers}

class TypesSuite extends FunSuite with Matchers {

  test("test of Spark Decimal types to Exasol Decimal types") {
    assert(exasolTypeFromSparkDataType(DecimalType.apply(5, 2)) === "DECIMAL(5,2)")
    assert(exasolTypeFromSparkDataType(DecimalType.SYSTEM_DEFAULT) === "DECIMAL(36,18)")
    assert(exasolTypeFromSparkDataType(DecimalType.apply(38, 37)) === "DECIMAL(36,36)")
  }

  test("test of Int type conversion") {
    assert(createSparkTypeFromSQLType(java.sql.Types.TINYINT, 0, 0, false) === ShortType)
    assert(createSparkTypeFromSQLType(java.sql.Types.SMALLINT, 0, 0, false) === ShortType)

    assert(createSparkTypeFromSQLType(java.sql.Types.INTEGER, 0, 0, true) === IntegerType)
    assert(createSparkTypeFromSQLType(java.sql.Types.INTEGER, 0, 0, false) === LongType)
  }

  test("test of DECIMAL and NUMERIC type conversion") {
    // DECIMAL
    assert(
      createSparkTypeFromSQLType(java.sql.Types.DECIMAL, 0, 0, true)
        === DecimalType.SYSTEM_DEFAULT
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.DECIMAL, 0, 0, false)
        === DecimalType.SYSTEM_DEFAULT
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.DECIMAL, 4, 1, false)
        === DecimalType(4, 1)
    )

    assert(
      createSparkTypeFromSQLType(java.sql.Types.DECIMAL, 36, 10, false)
        === DecimalType(getMaxPrecisionExasol(), 10)
    )

    assert(
      createSparkTypeFromSQLType(java.sql.Types.DECIMAL, 36, 36, false)
        === DecimalType(getMaxPrecisionExasol(), getMaxScaleExasol())
    )

    // NUMERIC
    assert(
      createSparkTypeFromSQLType(java.sql.Types.NUMERIC, 0, 0, true)
        === DecimalType.SYSTEM_DEFAULT
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.NUMERIC, 0, 0, false)
        === DecimalType.SYSTEM_DEFAULT
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.NUMERIC, 4, 1, false)
        === DecimalType(4, 1)
    )
  }

  test("test of DOUBLE, FLOAT and REAL type conversion") {
    assert(
      createSparkTypeFromSQLType(java.sql.Types.DOUBLE, 0, 0, false)
        === DoubleType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.FLOAT, 0, 0, false)
        === DoubleType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.REAL, 0, 0, false)
        === FloatType
    )
  }

  test("test of Stings type conversion") {
    assert(
      createSparkTypeFromSQLType(java.sql.Types.CHAR, 0, 0, false)
        === StringType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.NCHAR, 0, 0, false)
        === StringType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.VARCHAR, 0, 0, false)
        === StringType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.NVARCHAR, 0, 0, false)
        === StringType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.LONGVARCHAR, 0, 0, false)
        === StringType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.LONGNVARCHAR, 0, 0, false)
        === StringType
    )
  }

  test("test of Binaries type conversion") {
    assert(
      createSparkTypeFromSQLType(java.sql.Types.BINARY, 0, 0, false)
        === BinaryType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.VARBINARY, 0, 0, false)
        === BinaryType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.LONGVARBINARY, 0, 0, false)
        === BinaryType
    )
  }

  test("test of Booleans type conversion") {
    assert(
      createSparkTypeFromSQLType(java.sql.Types.BIT, 0, 0, false)
        === BooleanType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.BOOLEAN, 0, 0, false)
        === BooleanType
    )
  }

  test("test of Datetime type conversion") {
    assert(
      createSparkTypeFromSQLType(java.sql.Types.DATE, 0, 0, false)
        === DateType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.TIME, 0, 0, false)
        === TimestampType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.TIMESTAMP, 0, 0, false)
        === TimestampType
    )
  }

  test("test of Others type conversion") {
    assert(
      createSparkTypeFromSQLType(java.sql.Types.ROWID, 0, 0, false)
        === LongType
    )
    assert(
      createSparkTypeFromSQLType(java.sql.Types.STRUCT, 0, 0, false)
        === StringType
    )
  }

  test("`createTableSchema` should create a comma separated column names and types") {
    val schema: StructType = StructType(
      Seq(
        StructField("bool_col", BooleanType),
        StructField("str_col", StringType),
        StructField("int_col", IntegerType, false),
        StructField("float_col", FloatType),
        StructField("double_col", DoubleType),
        StructField("date_col", DateType),
        StructField("timestamp_col", TimestampType)
      )
    )

    val expectedStr =
      """"bool_col" BOOLEAN, "str_col" CLOB, "int_col" INTEGER NOT NULL, "float_col" FLOAT, """ +
        """"double_col" DOUBLE, "date_col" DATE, "timestamp_col" TIMESTAMP"""

    assert(createTableSchema(schema) === expectedStr)
  }

}
