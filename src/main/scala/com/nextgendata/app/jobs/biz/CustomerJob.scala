package com.nextgendata.app.jobs.biz

import com.nextgendata.app.maps._
import com.nextgendata.app.source.biz.{Customer => SrcBizCustomer}
import com.nextgendata.app.target.{Customer => TrgCustomer, CustomerRow => TrgCustomerRow}
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

    val dflInvLogPcm = Job.sc.broadcast(new ProvinceCodeMap(ProvinceCodeMap())
      with Logging[ProvinceCodeMapKey, ProvinceCodeMapVal] with StdOutLogging[ProvinceCodeMapKey, ProvinceCodeMapVal]
      with ApplyDefaultInvalid[ProvinceCodeMapKey, ProvinceCodeMapVal])

    val dflInvLogCntrPcm = Job.sc.broadcast(new CountryCodeMap(CountryCodeMap())
      with Logging[CountryCodeMapKey, CountryCodeMapVal] with StdOutLogging[CountryCodeMapKey, CountryCodeMapVal]
      with ApplyDefaultInvalid[CountryCodeMapKey, CountryCodeMapVal])

    val trgCustomerRows = customers.map(srcCust => {
      //Mapping the tuple to named values instead of using _1, _2, _3 syntax
      val (bizCustRow, cifXrefRow, cifCustRow) = srcCust

      val provCd = dflInvLogPcm.value.get(ProvinceCodeMapKey(bizCustRow.province)).getOrElse(dflInvLogPcm.value.getDefault)

      val cntryCd = dflInvLogCntrPcm.value.get(CountryCodeMapKey(provCd.countryCd)).getOrElse(dflInvLogCntrPcm.value.getDefault)

      if (provCd.provinceName == "-99" ||
        cntryCd.countryName == "-99") {
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
        cntryCd.countryName,
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