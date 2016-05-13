package com.nextgendata.app.jobs.biz

import com.nextgendata.Job
import com.nextgendata.app.maps._
import org.apache.spark.{SparkConf, SparkContext}
import com.nextgendata.app.source.biz.{Customer => SrcBizCustomer}
import com.nextgendata.app.target.{Customer => TrgCustomer, CustomerRow => TrgCustomerRow}
import com.nextgendata.maps.{Logging, StdOutLogging}

/**
  * Created by Craig on 2016-04-29.
  */
object CustomerJob extends Job {
  def main(args: Array[String]) {
    //TODO Need to make logic in the job code testable

    val sqlContext = Job.sqlContext
    // this is used to implicitly convert an RDD to a DataFrame or Dataset.
    import sqlContext.implicits._

    val customers = SrcBizCustomer.getCustomersWithCif

    val dflInvLogPcm = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]

    val trgCustomerRows = customers.map(srcCust => {
      //Mapping the tuple to named values instead of using _1, _2, _3 syntax
      val (bizCustRow, cifXrefRow, cifCustRow) = srcCust

      val provCd = dflInvLogPcm.get(ProvinceCodeMapKey(bizCustRow.province))

      TrgCustomerRow(
        bizCustRow.email,
        provCd.map(p => p.provinceCd).getOrElse(""),
        provCd.map(p => p.provinceName).getOrElse(""),
        bizCustRow.postal,
        cifCustRow.CIFId
      )
    })

    TrgCustomer.insert(trgCustomerRows)
  }
}
