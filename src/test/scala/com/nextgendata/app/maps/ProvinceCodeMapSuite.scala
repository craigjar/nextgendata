package com.nextgendata.app.maps

import com.nextgendata.{SharedSparkContext, SparkFunSuite}
import com.nextgendata.maps.{Logging, StdOutLogging}
import org.apache.spark.sql.SQLContext
//import com.nextgendata.{SharedSparkContext, SparkFunSuite}

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceCodeMapSuite extends SparkFunSuite with SharedSparkContext{
  test ("Test Mapper") {
    val sqlContext = new SQLContext(sc)

    val pcm = new ProvinceCodeMap(sqlContext)

    assert(pcm.lookup("ON").asInstanceOf[ProvinceCodeMapRow].provinceCd == "7")

    assert(pcm.lookup("YT").asInstanceOf[ProvinceCodeMapRow].provinceCd == "13")

    assert(pcm.lookup("XX") == ())

    val pcmWithLogger = new ProvinceCodeMap(sqlContext) with Logging with StdOutLogging

    assert(pcmWithLogger.lookup("XX") == ())

    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(sqlContext) with Logging with StdOutLogging with ApplyDefaultInvaild

    assert(pcmWithLoggerAndDefaultInvalid.lookup("XX") == "-1")
  }
}