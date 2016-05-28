package com.nextgendata.framework.maps

/**
  * Created by Craig on 2016-04-27.
  *
  * This is a mixin trait to complete the Logging.log implementation with a simple println to stdout.
  *
  */
trait StdOutLogging[K, V] extends Logging[K, V]{
  /**
    * Uses println to log a message to stdout.  Example:
    *
    * `Source value "ABC" did not map.`
    *
    * @param srcVal
    * @param mappedVal
    */
  override def log(srcVal: K, mappedVal: Option[V]): Unit = {
    println("""Source value """" + srcVal.toString + """" did not map.""")
  }
}
