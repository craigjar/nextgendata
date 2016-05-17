package com.nextgendata.app.maps

import com.nextgendata.framework.maps.{Mapper, _}
import org.apache.spark.sql.SQLContext

/**
  * Created by Justin on 5/16/2016.
  */
class CountryCodeMap(pcm: Map[CountryCodeMapKey, CountryCodeMapVal])
  extends Mapper[CountryCodeMapKey, CountryCodeMapVal] {

  def +[V1 >: CountryCodeMapVal](kv: (CountryCodeMapKey, V1)) = pcm + kv
  def -(key: CountryCodeMapKey) = new CountryCodeMap(pcm - key)

  def get(key: CountryCodeMapKey) = pcm.get(key)
  def iterator = pcm.iterator

  override def getDefault: CountryCodeMapVal = CountryCodeMapVal("-99")
  override def getInvalid: CountryCodeMapVal = CountryCodeMapVal("-1")
  override def getEmptyKey: CountryCodeMapKey = CountryCodeMapKey("")
}

case class CountryCodeMapKey(srcCountryCd: String)
case class CountryCodeMapVal(countryName: String)

object CountryCodeMap {
  private var _data : CountryCodeMap = null

  def apply(sqlContext: SQLContext): CountryCodeMap = {
    if (_data == null) {
      val sc = sqlContext.sparkContext

      val file = sc.textFile("examples/spark_repl_demo/country_code_map.txt")

      //Have to wrap this Spark code in a block so that the header val is only scoped to this call
      //not the entire class.  If header val was class level, then the closure would try to serialize
      //this entire class and fail since it contains non-serializable objects (SQL/SparkContext)
      val countryCodeMap = {
        val header = file.first()

        file
          .filter(line => line != header)
          .map(_.split("\t"))
          .map(p => (CountryCodeMapKey(p(0)), CountryCodeMapVal(p(1)))) //Need to make a tuple (key, value) to access pairRDD functions
          .cache()
      }
      _data = new CountryCodeMap(countryCodeMap.collectAsMap().toMap)
    }

    _data
  }
}
