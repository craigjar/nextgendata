package com.nextgendata.framework.maps

/**
  * Created by Craig on 2016-04-27.
  */
trait Logging[K, V] extends Mapper[K, V] {
  abstract override def get(srcVal: K): Option[V] = {
    val mappedVal = super.get(srcVal)

    if (mappedVal.isEmpty) log(srcVal, mappedVal)

    mappedVal
  }

  def log (srcVal: K, mappedVal: Option[V]): Unit
}
