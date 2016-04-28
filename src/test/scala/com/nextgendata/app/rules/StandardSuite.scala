package com.nextgendata.app.rules

import com.nextgendata.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.udf


/**
  * Created by Craig on 2016-04-17.
  */
class StandardSuite extends SparkFunSuite with SharedSparkContext {
  test("defaultInvalid test mapped value") {
    // If value is mapped (mappedVal != null) then should return mapped value
    assert(Standard.defaultInvalid("1", "2") == "2")
  }

  test("defaultInvalid test default value") {
    // If source value is null then default
    assert(Standard.defaultInvalid((), "2") == "-99")
  }

  test("defaultInvalid test empty String for source value") {
    // If source value was not provided (empty String) then default
    assert(Standard.defaultInvalid("", ()) == "-99")
  }

  test("defaultInvalid test invalid value") {
    // If source value was provided (not null) but not mapped (mappedVal = null) then invalid
    assert(Standard.defaultInvalid("1", ()) == "-1")
  }

  test("defaultInvalid test works in SparkContext UDF") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.parallelize(Array(Array("1", "2"),Array(null, "2"),Array("1",null))).map(d => DataRec(d(0), d(1))).toDF()

    val defaultInvalid = udf((srcVal: String, mappedVal: String) => Standard.defaultInvalid(srcVal, mappedVal).asInstanceOf[String])

    val resData = data
      .withColumn("result", defaultInvalid($"srcVal", $"mappedVal"))
      .take(3)

    assert(resData(0)(2) == "2")
    assert(resData(1)(2) == "-99")
    assert(resData(2)(2) == "-1")
  }
}

case class DataRec(srcVal: String, mappedVal: String)
