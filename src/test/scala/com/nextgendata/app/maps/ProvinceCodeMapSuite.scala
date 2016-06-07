package com.nextgendata.app.maps

import com.nextgendata.framework.maps.{Logging, StdOutLogging}
import com.nextgendata.{SharedSparkContext, SparkFunSuite}

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceCodeMapSuite extends SparkFunSuite with SharedSparkContext {

  test ("Test plain ProvinceCodeMap with valid value") {
    val pcm = new ProvinceCodeMap(ProvinceCodeMap())
    assert(pcm.get(ProvinceCodeMapKey("ON")).get.provinceCd == "7")
  }

  test ("Test plain ProvinceCodeMap with invalid value") {
    val pcm = new ProvinceCodeMap(ProvinceCodeMap())
    assert(pcm.get(ProvinceCodeMapKey("XX")).isEmpty)
  }

  test ("Test plain ProvinceCodeMap to ensure header is filtered out") {
    val pcm = new ProvinceCodeMap(ProvinceCodeMap())
    assert(pcm.get(ProvinceCodeMapKey("src_province_cd")).isEmpty)
  }

  test ("Test logger decorated ProvinceCodeMap with invalid value") {
    val pcmWithLogger = new ProvinceCodeMap(ProvinceCodeMap())
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLogger.get(ProvinceCodeMapKey("XX")).isEmpty)
  }

  test ("Test logger decorated ProvinceCodeMap with valid value") {
    val pcmWithLogger = new ProvinceCodeMap(ProvinceCodeMap())
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLogger.get(ProvinceCodeMapKey("ON")).get.provinceCd == "7")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with invalid value") {
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap())
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLoggerAndDefaultInvalid.get(ProvinceCodeMapKey("XX")).get.provinceCd == "-1")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with valid value") {
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap())
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLoggerAndDefaultInvalid.get(ProvinceCodeMapKey("ON")).get.provinceCd == "7")
  }

  test ("Test logger & default/invalid decorated ProvinceCodeMap with no value") {
    val pcmWithLoggerAndDefaultInvalid = new ProvinceCodeMap(ProvinceCodeMap())
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]
    assert(pcmWithLoggerAndDefaultInvalid.get(ProvinceCodeMapKey("")).get.provinceCd == "-99")
  }

  test ("Test overridden Map method +") {
    val pcm = new ProvinceCodeMap(ProvinceCodeMap())
    val pcm2 = pcm + (ProvinceCodeMapKey("XX") -> ProvinceCodeMapVal("14", "Test", "123"))
    assert(pcm2.get(ProvinceCodeMapKey("XX")).get.provinceCd == "14")
  }

  test ("Test overridden Map method -") {
    val pcm = new ProvinceCodeMap(ProvinceCodeMap())
    val pcm2 = pcm - ProvinceCodeMapKey("ON")
    assert(pcm2.get(ProvinceCodeMapKey("ON")).isEmpty)
  }

  test ("Test overridden Map method iterator") {
    val pcm = new ProvinceCodeMap(ProvinceCodeMap())
    val iter = pcm.iterator
    iter.foreach(elem => assert(pcm.get(elem._1).nonEmpty))
  }
}