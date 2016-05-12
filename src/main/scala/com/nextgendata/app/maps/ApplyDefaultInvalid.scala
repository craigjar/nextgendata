package com.nextgendata.app.maps

import com.nextgendata.maps.Mapper
import com.nextgendata.app.rules.Standard

/**
  * Created by Craig on 2016-04-28.
  */
trait ApplyDefaultInvalid[K, V] extends Mapper[K, V]{
  abstract override def get(srcVal: K): Option[V] = {
    val mappedVal = super.get(srcVal)

    if (srcVal == () || srcVal == null || srcVal == this.getEmptyKey)
      Option(this.getDefault)
    else if (srcVal.isInstanceOf[String] && srcVal.asInstanceOf[String].trim().isEmpty())
      Option(this.getDefault)
    else if (mappedVal.isEmpty)
      Option(this.getInvalid)
    else
      mappedVal
  }
}
