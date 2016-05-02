package com.nextgendata.maps

/**
  * Created by Craig on 2016-04-26.
  */
trait Mapper {
  //TODO apply generics instead of using Any
  def lookup(srcVal: Any): Any
}
