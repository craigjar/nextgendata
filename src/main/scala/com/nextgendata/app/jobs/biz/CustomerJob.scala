package com.nextgendata.app.jobs.biz

import com.nextgendata.app.maps.{ApplyDefaultInvaild, ProvinceCodeMap, ProvinceCodeMapVal}
import org.apache.spark.{SparkConf, SparkContext}
import com.nextgendata.app.source.biz.{Customer => SrcBizCustomer}
import com.nextgendata.app.target.{Customer => TrgCustomer, CustomerRow => TrgCustomerRow}
import com.nextgendata.maps.{Logging, StdOutLogging}

/**
  * Created by Craig on 2016-04-29.
  */
object CustomerJob {
  def main(args: Array[String]) {
    //TODO Need to make logic in the job code testable
    val conf = new SparkConf().setAppName("MyLocalApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame or Dataset.
    import sqlContext.implicits._

    val customers = SrcBizCustomer.getCustomers(sqlContext)

    val dflInvLogPcm = new ProvinceCodeMap(ProvinceCodeMap.init(sqlContext))
      with Logging with StdOutLogging
      with ApplyDefaultInvaild

    val trgCustomerRows = customers.map(srcCust =>
      TrgCustomerRow(
        srcCust.email,
        (dflInvLogPcm.lookup(srcCust.province) match{
          case pcmv: ProvinceCodeMapVal => pcmv.provinceCd
          case _ =>
        }).toString
      )
    )

    TrgCustomer.insert(trgCustomerRows)
  }
}
