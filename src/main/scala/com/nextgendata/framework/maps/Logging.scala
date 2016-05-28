package com.nextgendata.framework.maps

/**
  * Created by Craig on 2016-04-27.
  *
  * This is a stackable trait or decorator that can be mixed in with a [[Mapper]]
  * implementation.
  *
  * It will detect when a get does not return a value (lookup fails) and then invoke
  * the log function.
  *
  * The log function is left abstract so that the application can implement as needed, for
  * example write a message into their database, or application log file.  Due to this, the
  * application must mix in an additional trait which implements this function.
  *
  * Example:
  * {{{
  * val myMapper = Job.sc.broadcast(new MyMap(MyMap(sqlContext))
  *    with Logging[MyMapKey, MyMapVal] with StdOutLogging[MyMapKey, MyMapVal]
  * }}}
  */
trait Logging[K, V] extends Mapper[K, V] {
  /**
    * This method intercepts a Mapper's get call and detects when the lookup fails (returns
    * and empty result) then call the log function to record this failed lookup.
    * @param srcVal
    * @return
    */
  abstract override def get(srcVal: K): Option[V] = {
    val mappedVal = super.get(srcVal)

    if (mappedVal.isEmpty) log(srcVal, mappedVal)

    mappedVal
  }

  def log (srcVal: K, mappedVal: Option[V]): Unit
}
