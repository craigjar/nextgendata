package com.nextgendata.app.source.biz

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by Craig on 2016-04-29.
  */
object Customer {
  def getCustomers(sqlContext: SQLContext): RDD[CustomerRow] = {

    val sc = sqlContext.sparkContext

    val file = sc.textFile("examples/spark_repl_demo/ca-500.txt")

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

    customers
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
