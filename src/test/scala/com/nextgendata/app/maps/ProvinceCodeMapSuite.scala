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
    val pcm = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
    assert(pcm.get(ProvinceCodeMapKey("ON")).get.provinceCd == "7")
  }

  test ("Test plain ProvinceCodeMap with invalid value") {
    val sqlContext = new SQLContext(sc)
    val pcm = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
    assert(pcm.get(ProvinceCodeMapKey("XX")).isEmpty)
  }

  test ("Test plain ProvinceCodeMap to ensure header is filtered out") {
    val sqlContext = new SQLContext(sc)
    val pcm = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
    assert(pcm.get(ProvinceCodeMapKey("src_province_cd")).isEmpty)
  }

  test ("Test logger decorated ProvinceCodeMap with invalid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLogger = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLogger.get(ProvinceCodeMapKey("XX")).isEmpty)
  }

  test ("Test logger decorated ProvinceCodeMap with valid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLogger = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLogger.get(ProvinceCodeMapKey("ON")).get.provinceCd == "7")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with invalid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLoggerAndDefaultInvalid.get(ProvinceCodeMapKey("XX")).get.provinceCd == "-1")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with valid value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLoggerAndDefaultInvalid.get(ProvinceCodeMapKey("ON")).get.provinceCd == "7")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with no value") {
    val sqlContext = new SQLContext(sc)
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLoggerAndDefaultInvalid.get(ProvinceCodeMapKey("")).get.provinceCd == "-99")
  }
}