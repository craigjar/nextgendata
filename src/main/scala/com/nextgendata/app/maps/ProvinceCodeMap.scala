package com.nextgendata.app.maps

import com.nextgendata.maps._
import org.apache.spark.sql.SQLContext

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceCodeMap(sqlContext: SQLContext) extends Mapper {
  val sc = sqlContext.sparkContext

  val provinceCodeMap = sc.textFile("examples/spark_repl_demo/province_code_map.txt")
    .map(_.split("\t"))
    .map(p => (p(1), ProvinceCodeMapRow(p(0), p(1)))) //Need to make a tuple (key, value) to access pairRDD functions

  provinceCodeMap.cache()

  override def lookup(srcVal: Any): Any = {

    val ret = provinceCodeMap.lookup(srcVal.toString)

    if (ret.nonEmpty) ret.head  //returns the first item found
  }
}

case class ProvinceCodeMapRow(provinceCd: String, srcProvinceCd: String)
