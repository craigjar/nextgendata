package com.nextgendata.rules

/**
  * Created by Craig on 2016-04-16.
  */
class Standard {
  def defaultInvalid(origVal: String, mappedVal: String): String = {
    if (origVal == null || origVal.trim() == "") "-99"
    else if (mappedVal == null) "-1"
    else mappedVal
  }
}

object Standard {
  private var _instance  : Standard = null
  def instance() = {
    if (_instance == null)
      _instance = new Standard()
    _instance
  }
}