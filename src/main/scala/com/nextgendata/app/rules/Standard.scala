package com.nextgendata.app.rules

/**
  * Created by Craig on 2016-04-16.
  */

object Standard {
  def defaultInvalid(origVal: Any, mappedVal: Any): Any = (origVal, mappedVal) match {
    case (null, _)  => "-99"
    case (orig: String, _) if orig.trim.isEmpty => "-99"
    case (_, null)  => "-1"
    case (_, mapped) => mapped
  }
}