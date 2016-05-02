package com.nextgendata.maps

/**
  * Created by Craig on 2016-04-27.
  */
trait Logging extends Mapper {
  abstract override def lookup(srcVal: Any): Any = {
    val mappedVal = super.lookup(srcVal)

    if (mappedVal == ()) log(srcVal, mappedVal)

    mappedVal
  }

  def log (srcVal: Any, mappedVal: Any): Unit
}
