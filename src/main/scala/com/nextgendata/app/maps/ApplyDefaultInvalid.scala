package com.nextgendata.app.maps

import com.nextgendata.framework.maps.Mapper
import com.nextgendata.app.rules.Standard

/**
  * Created by Craig on 2016-04-28.
  */
trait ApplyDefaultInvalid[K, V] extends Mapper[K, V]{
  abstract override def get(srcVal: K): Option[V] = {
    val mappedVal = super.get(srcVal)

    (srcVal, mappedVal) match {
      case (null, _)  => Option(this.getDefault) // compiler warns us here against using null.
      case (s: String, _) if s.trim.isEmpty => Option(this.getDefault)
      case (k, _)  if k == this.getEmptyKey => Option(this.getDefault)
      case (_, m) if m.isEmpty => Option(this.getInvalid)
      case _  => mappedVal
    }
  }
}
