package com.nextgendata.app.maps

import com.nextgendata.maps._
import org.apache.spark.sql.SQLContext
import scala.collection.Map

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceCodeMap (provinceCodeMapRows: Map[ProvinceCodeMapKey, ProvinceCodeMapVal]) extends Serializable with Mapper {
  val provinceCodeMap = provinceCodeMapRows

  override def lookup(srcVal: Any): Any = {

    val ret = provinceCodeMap.get(ProvinceCodeMapKey(srcVal.asInstanceOf[String]))

    if (ret.nonEmpty) ret.head  //returns the first item found
  }
}

case class ProvinceCodeMapKey(srcProvinceCd: String)
case class ProvinceCodeMapVal(provinceCd: String)

object ProvinceCodeMap {
  private var _data : Map[ProvinceCodeMapKey, ProvinceCodeMapVal] = null

  def init(sqlContext: SQLContext): Map[ProvinceCodeMapKey, ProvinceCodeMapVal] = {
    if (_data == null) {
      val sc = sqlContext.sparkContext

      val file = sc.textFile("examples/spark_repl_demo/province_code_map.txt")

      //Have to wrap this Spark code in a block so that the header val is only scoped to this call
      //not the entire class.  If header val was class level, then the closure would try to serialize
      //this entire class and fail since it contains non-serializable objects (SQL/SparkContext)
      val provinceCodeMap = {
        val header = file.first()

        file
          .filter(line => line != header)
          .map(_.split("\t"))
          .map(p => (ProvinceCodeMapKey(p(1)), ProvinceCodeMapVal(p(0)))) //Need to make a tuple (key, value) to access pairRDD functions
          .cache()
      }

      _data = provinceCodeMap.collectAsMap()
    }

    _data
  }
}
