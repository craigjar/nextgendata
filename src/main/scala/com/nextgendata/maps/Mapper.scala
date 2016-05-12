package com.nextgendata.maps

/**
  * Created by Craig on 2016-04-26.
  */
trait Mapper[K, V] extends Map[K, V] with Serializable {

  def getDefault: V
  def getInvalid: V
  def getEmptyKey: K
}
