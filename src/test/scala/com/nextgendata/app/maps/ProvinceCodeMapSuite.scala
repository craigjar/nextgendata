package com.nextgendata.app.maps

import com.nextgendata.maps.{Logging, StdOutLogging}
import com.nextgendata.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql.SQLContext

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceCodeMapSuite extends SparkFunSuite with SharedSparkContext {

  test ("Test plain ProvinceCodeMap with valid value") {
    val sqlContext = new SQLContext(sc)
    //val pcm = new ProvinceCodeMap(sqlContext)
    val pcm = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
    assert(pcm.lookup("ON").asInstanceOf[ProvinceCodeMapVal].provinceCd == "7")
  }

  test ("Test plain ProvinceCodeMap with invalid value") {
    val sqlContext = new SQLContext(sc)
    val pcm = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
    assert(pcm.lookup("XX") == ())
  }

  test ("Test plain ProvinceCodeMap to ensure header is filtered out") {
    val sqlContext = new SQLContext(sc)
    val pcm = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
    assert(pcm.lookup("src_province_cd") == ())
  }

  test ("Test logger decorated ProvinceCodeMap with invalid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLogger = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
      with Logging with StdOutLogging
    assert(pcmWithLogger.lookup("XX") == ())
  }

  test ("Test logger decorated ProvinceCodeMap with valid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLogger = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
      with Logging with StdOutLogging
    assert(pcmWithLogger.lookup("ON").asInstanceOf[ProvinceCodeMapVal].provinceCd == "7")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with invalid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
      with Logging with StdOutLogging
      with ApplyDefaultInvaild
    assert(pcmWithLoggerAndDefaultInvalid.lookup("XX") == "-1")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with valid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
      with Logging with StdOutLogging
      with ApplyDefaultInvaild
    assert(pcmWithLoggerAndDefaultInvalid.lookup("ON").asInstanceOf[ProvinceCodeMapVal].provinceCd == "7")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with no value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
      with Logging with StdOutLogging
      with ApplyDefaultInvaild
    assert(pcmWithLoggerAndDefaultInvalid.lookup("") == "-99")
  }
}