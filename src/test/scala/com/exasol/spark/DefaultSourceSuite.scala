package com.exasol.spark

import org.apache.spark.sql.SQLContext

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.mockito.MockitoSugar

class DefaultSourceSuite extends FunSuite with Matchers with MockitoSugar {

  test("when reading should throw an Exception if no `query` parameter is provided") {
    val sqlContext = mock[SQLContext]

    val thrown = intercept[UnsupportedOperationException] {
      new DefaultSource().createRelation(sqlContext, Map[String, String]())
    }

    assert(
      thrown.getMessage === "A query parameter should be specified in order to run the operation"
    )
  }

  test("mergeConfigurations should merge runtime sparkConf into user provided parameters") {
    val sparkConf = Map[String, String](
      "spark.exasol.username" -> "newUsername",
      "spark.exasol.host" -> "hostName",
      "spark.other.options" -> "irrelevance"
    )
    val parameters = Map[String, String]("username" -> "oldUsername", "password" -> "oldPassword")

    val newConf = new DefaultSource().mergeConfigurations(parameters, sparkConf)
    // overwrite config if both are provided
    assert(newConf.getOrElse("username", "not available") === "newUsername")

    // use config from parameters if sparkConf doesn't provide
    assert(newConf.getOrElse("password", "some random password") === "oldPassword")

    // use config from sparkConf if parameters doesn't provide
    assert(newConf.getOrElse("host", "some random host") === "hostName")

    // should not contains irrelevant options for exasol
    assert(!newConf.contains("spark.other.options") && !newConf.contains("options"))
  }
}
