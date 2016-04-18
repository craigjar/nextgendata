package com.nextgendata.rules

import com.nextgendata.{SharedSparkContext, SparkFunSuite}

/**
  * Created by Craig on 2016-04-17.
  */
class StandardSuite extends SparkFunSuite with SharedSparkContext {
  test("defaultInvalid test mapped value") {
    // If value is mapped (mappedVal != null) then should return mapped value
    assert(Standard.instance().defaultInvalid("1", "2") == "2")
  }

  test("defaultInvalid test default value") {
    // If source value is null then default
    assert(Standard.instance().defaultInvalid(null, "2") == "-99")
  }

  test("defaultInvalid test invalid value") {
    // If source value was provided (not null) but not mapped (mappedVal = null) then invalid
    assert(Standard.instance().defaultInvalid("1", null) == "-1")
  }

  test("defaultInvalid test works in SparkContext UDF") {
    //TODO
  }
}
