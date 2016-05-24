package com.nextgendata.app.target

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Dataset

/**
  * Created by Craig on 2016-04-29.
  */
object Customer {
  def insert(customers: Dataset[CustomerRow]): Unit ={
    FileUtils.deleteDirectory(new File("target/Customer.txt"))
    customers.rdd.saveAsTextFile("target/Customer.txt")
  }
}

object BadCustomer {
  def insert(customers: Dataset[BadCustomerRow]): Unit ={
    FileUtils.deleteDirectory(new File("target/BadCustomer.txt"))
    customers.rdd.saveAsTextFile("target/BadCustomer.txt")
  }
}

case class CustomerRow(email: String, provinceCode: String, provinceName:String, countryName:String, postal: String, CIFId: Int)

case class BadCustomerRow(email: String, postal: String, CIFId: Int)
