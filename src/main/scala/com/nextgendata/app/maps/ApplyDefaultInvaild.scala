package com.nextgendata.app.maps

import com.nextgendata.maps.Mapper
import com.nextgendata.app.rules.Standard

/**
  * Created by Craig on 2016-04-28.
  */
trait ApplyDefaultInvaild extends Mapper{
  abstract override def lookup(srcVal: Any): Any = {
    val mappedVal = super.lookup(srcVal)

    Standard.defaultInvalid(srcVal, mappedVal)
  }

}
