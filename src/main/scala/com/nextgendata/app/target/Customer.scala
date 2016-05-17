package com.nextgendata.app.target

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD

/**
  * Created by Craig on 2016-04-29.
  */
object Customer {
  def insert(customers: RDD[CustomerRow]): Unit ={
    FileUtils.deleteDirectory(new File("target/Customer.txt"))
    customers.saveAsTextFile("target/Customer.txt")
  }
}

object BadCustomer {
  def insert(customers: RDD[BadCustomerRow]): Unit ={
    FileUtils.deleteDirectory(new File("target/BadCustomer.txt"))
    customers.saveAsTextFile("target/BadCustomer.txt")
  }
}

case class CustomerRow(email: String, provinceCode: String, provinceName:String, countryName:String, postal: String, CIFId: Int)

case class BadCustomerRow(email: String, postal: String, CIFId: Int)
