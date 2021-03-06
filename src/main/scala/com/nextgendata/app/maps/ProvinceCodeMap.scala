package com.nextgendata.app.maps

import com.nextgendata.framework.Job
import com.nextgendata.framework.maps._

/**
  * Created by Craig on 2016-04-27.
  */
class ProvinceCodeMap(pcm: Map[ProvinceCodeMapKey, ProvinceCodeMapVal])
  extends Mapper[ProvinceCodeMapKey, ProvinceCodeMapVal] {

  def +[V1 >: ProvinceCodeMapVal](kv: (ProvinceCodeMapKey, V1)): Map[ProvinceCodeMapKey, V1] = pcm + kv
  def -(key: ProvinceCodeMapKey): ProvinceCodeMap = new ProvinceCodeMap(pcm - key)

  def get(key: ProvinceCodeMapKey): Option[ProvinceCodeMapVal]  = pcm.get(key)
  def iterator: Iterator[(ProvinceCodeMapKey, ProvinceCodeMapVal)] = pcm.iterator

  override def getDefault: ProvinceCodeMapVal = ProvinceCodeMapVal("-99", "-99","-99")
  override def getInvalid: ProvinceCodeMapVal = ProvinceCodeMapVal("-1", "-1","-1")
  override def getEmptyKey: ProvinceCodeMapKey = ProvinceCodeMapKey("")
}

case class ProvinceCodeMapKey(srcProvinceCd: String)
case class ProvinceCodeMapVal(provinceCd: String, provinceName: String, countryCd: String)

object ProvinceCodeMap {
  private lazy val _data = init

  def init: ProvinceCodeMap = {
    val sc = Job.sqlContext.sparkContext
    val file = sc.textFile("examples/spark_repl_demo/province_code_map.txt")

    //Have to wrap this Spark code in a block so that the header val is only scoped to this call
    //not the entire class.  If header val was class level, then the closure would try to serialize
    //this entire class and fail since it contains non-serializable objects (SQL/SparkContext)
    val provinceCodeMap = {
      val header = file.first()

      file
        .filter(line => line != header)
        .map(_.split("\t"))
        .map(p => (ProvinceCodeMapKey(p(1)), ProvinceCodeMapVal(p(0), p(2),p(3)))) //Need to make a tuple (key, value) to access pairRDD functions
        .cache()
    }
    new ProvinceCodeMap(provinceCodeMap.collectAsMap().toMap)
  }
  def apply(): ProvinceCodeMap = _data

}
