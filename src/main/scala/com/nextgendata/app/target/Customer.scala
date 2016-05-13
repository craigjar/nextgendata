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

case class CustomerRow(email: String, provinceCode: String, provinceName:String, postal: String, CIFId: Int)
