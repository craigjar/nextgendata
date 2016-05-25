package com.nextgendata.app.jobs.biz

import com.nextgendata.app.maps._
import org.apache.spark.{SparkConf, SparkContext}
import com.nextgendata.app.source.biz.{Customer => SrcBizCustomer}
import com.nextgendata.app.target.{Customer => TrgCustomer, BadCustomer => TrgBadCustomer, CustomerRow => TrgCustomerRow, BadCustomerRow => TrgBadCustomerRow}
import com.nextgendata.framework.Job
import com.nextgendata.framework.maps.{Logging, StdOutLogging}

/**
  * Created by Craig on 2016-04-29.
  */
object CustomerJob extends Job {
  def main(args: Array[String]) {
    //TODO Need to make logic in the job code testable

    val sqlContext = Job.sqlContext
    // this is used to implicitly convert an RDD to a DataFrame or Dataset.
    import sqlContext.implicits._

    val rowInsertCnt = Job.sc.accumulator(0,"Customer Insert")
    val rowSkipCnt = Job.sc.accumulator(0,"Customer Skip")

    val customers = SrcBizCustomer.getCustomersWithCif

    //val countryMap = CountryCodeMapTbl.getCountryCodeMap

    val dflInvLogPcm = new ProvinceCodeMap(ProvinceCodeMap(sqlContext))
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal]

    val dflInvLogCntrPcm = new CountryCodeMap(CountryCodeMap(sqlContext))
      with Logging[CountryCodeMapKey, CountryCodeMapVal] with StdOutLogging[CountryCodeMapKey, CountryCodeMapVal]
      with ApplyDefaultInvalid[CountryCodeMapKey, CountryCodeMapVal]

    val trgCustomerRows = customers.map(srcCust => {
        //Mapping the tuple to named values instead of using _1, _2, _3 syntax
      val (bizCustRow, cifXrefRow, cifCustRow) = srcCust

      val provCd = dflInvLogPcm.get(ProvinceCodeMapKey(bizCustRow.province)).getOrElse(dflInvLogPcm.getDefault)

      val cntryCd = dflInvLogCntrPcm.get(CountryCodeMapKey(provCd.countryCd)).getOrElse(dflInvLogCntrPcm.getDefault)

      val countryRdd = Job.sc.parallelize(Seq(cntryCd))
      val broadcastedCntry = Job.sc.broadcast(cntryCd)

      if (provCd.provinceName == "-99" ||
        broadcastedCntry.value.countryName == "-99") {
        rowSkipCnt += 1
        //throw new InvalidRowException("Not Valid")
      }
      else {
        rowInsertCnt += 1
      }
      TrgCustomerRow(
        bizCustRow.email,
        provCd.provinceCd,
        provCd.provinceName,
        broadcastedCntry.value.countryName,
        bizCustRow.postal,
        cifCustRow.CIFId
      )
    })
    TrgCustomer.insert(trgCustomerRows)
    println("""Skipped Row Count """ + rowSkipCnt)
    println("""Inserted Row Count """ + rowInsertCnt)
  }
}

class InvalidRowException(smth: String) extends Exception(smth) {

}

