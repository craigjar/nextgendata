package com.nextgendata.maps

/**
  * Created by Craig on 2016-04-27.
  */
trait StdOutLogging[K, V] extends Logging[K, V]{
  override def log(srcVal: K, mappedVal: Option[V]): Unit = {
    println("""Source value """" + srcVal.toString + """" did not map.""")
  }
}
