package com.nextgendata.maps

/**
  * Created by Craig on 2016-04-26.
  */
trait Mapper {
  def lookup(srcVal: Any): Any
}
