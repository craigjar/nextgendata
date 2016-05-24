package com.nextgendata.app.source.biz

import org.apache.spark.sql.Dataset
import com.nextgendata.app.source.cif.{Customer => CifCustomer, CustomerRow => CifCustomerRow}
import com.nextgendata.app.source.cif.{Xref => CifXref, XrefRow => CifXrefRow}
import com.nextgendata.framework.Job
/**
  * Created by Craig on 2016-04-29.
  */
object Customer {
  def getCustomers: Dataset[CustomerRow] = {

    val file = Job.sc.textFile("examples/spark_repl_demo/ca-500.txt")

    val sqlContext = Job.sqlContext
    // this is used to implicitly convert an RDD to a DataFrame or Dataset.
    import sqlContext.implicits._

    //Have to wrap this Spark code in a block so that the header val is only scoped to this call
    //not the entire class.  If header val was class level, then the closure would try to serialize
    //this entire class and fail since it contains non-serializable objects (SQL/SparkContext)
    val customers = {
      val header = file.first()

      file
        .filter(line => line != header)
        .map(_.split("\t"))
        .map(p => CustomerRow(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10)))
        //.toDS()
    }

    customers.toDS
  }

  def getCustomersWithCif: Dataset[(CustomerRow, CifXrefRow, CifCustomerRow)] ={
    val sqlContext = Job.sqlContext
    // this is used to implicitly convert an RDD to a DataFrame or Dataset.
    import sqlContext.implicits._

    getCustomers.as("A")
      .joinWith(
        CifXref.getXref.filter(_.XrefSystem == "biz").as("B"),
        $"A.email" === $"B.XrefId",
        "left_outer"
      )
      .joinWith(
        CifCustomer.getCustomers.as("C"),
        $"_2.CIFId" === $"C.CIFId",
        "left_outer"
      )
      .map(r => (r._1._1, r._1._2, r._2))

/*
    val keyedCifCustomers = CifCustomer.getCustomers.keyBy(r => r.CIFId)

    val keyedBizCifXref = CifXref.getXref
      .filter(r => r.XrefSystem == "biz")
      .keyBy(r => r.CIFId)

    val keyedBizCifCustomers = keyedBizCifXref.join(keyedCifCustomers).keyBy(r => r._2._1.XrefId)

    val customersWithCif = getCustomers
      .keyBy(r => r.email)
      .join(keyedBizCifCustomers)
      .map(r => (r._2._1, r._2._2._2._1, r._2._2._2._2))

    customersWithCif
*/
  }
}

case class CustomerRow(firstName: String,
                       lastName: String,
                       companyName: String,
                       address: String,
                       city: String,
                       province: String,
                       postal: String,
                       phone1: String,
                       phone2: String,
                       email: String,
                       web: String)
