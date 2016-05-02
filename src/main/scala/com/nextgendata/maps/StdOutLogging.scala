package com.nextgendata.maps

/**
  * Created by Craig on 2016-04-27.
  */
trait StdOutLogging extends Logging{
  def log(srcVal: Any, mappedVal: Any): Unit = {
    println("""Source value """" + srcVal.toString + """" did not map.""")
  }
}
