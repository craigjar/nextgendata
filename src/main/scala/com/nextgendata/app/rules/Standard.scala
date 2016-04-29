package com.nextgendata.app.rules

/**
  * Created by Craig on 2016-04-16.
  */

object Standard {
  def defaultInvalid(origVal: Any, mappedVal: Any): Any = {
    if (origVal == () || origVal == null) "-99"
    else if (origVal.isInstanceOf[String] && origVal.asInstanceOf[String].trim().isEmpty()) "-99"
    else if (mappedVal == () || mappedVal == null) "-1"
    else mappedVal
  }
}