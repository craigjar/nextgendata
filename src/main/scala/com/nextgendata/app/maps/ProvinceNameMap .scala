package com.nextgendata.app.maps

import com.nextgendata.maps._
import org.apache.spark.sql.SQLContext
import scala.collection.Map

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceNameMap (ProvinceNameMapRows: Map[ProvinceNameMapKey, ProvinceNameMapVal]) extends Serializable with Mapper {
  val ProvinceNameMap = ProvinceNameMapRows

  override def lookup(srcVal: Any): Any = {

    val ret = ProvinceNameMap.get(ProvinceNameMapKey(srcVal.asInstanceOf[String]))

    if (ret.nonEmpty) ret.head  //returns the first item found
  }
}

case class ProvinceNameMapKey(srcProvinceCd: String)
case class ProvinceNameMapVal(provinceName: String)

object ProvinceNameMap {
  private var _data : Map[ProvinceNameMapKey, ProvinceNameMapVal] = null

  def init(sqlContext: SQLContext): Map[ProvinceNameMapKey, ProvinceNameMapVal] = {
    if (_data == null) {
      val sc = sqlContext.sparkContext

      val file = sc.textFile("examples/spark_repl_demo/province_code_map.txt")

      //Have to wrap this Spark code in a block so that the header val is only scoped to this call
      //not the entire class.  If header val was class level, then the closure would try to serialize
      //this entire class and fail since it contains non-serializable objects (SQL/SparkContext)
      val ProvinceNameMap = {
        val header = file.first()

        file
          .filter(line => line != header)
          .map(_.split("\t"))
          .map(p => (ProvinceNameMapKey(p(1)), ProvinceNameMapVal(p(2)))) //Need to make a tuple (key, value) to access pairRDD functions
          .cache()
      }

      _data = ProvinceNameMap.collectAsMap()
    }

    _data
  }
}
