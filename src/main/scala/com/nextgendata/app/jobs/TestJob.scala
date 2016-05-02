package com.nextgendata.jobs

import com.nextgendata.app.rules.Standard
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

/**
  * Created by Craig on 2016-04-18.
  */
object TestJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MyLocalApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create an RDD of People objects and register it as a table.
    val people = sc.textFile("examples/spark_repl_demo/ca-500.txt")
      .map(_.split("\t"))
      .map(p => Person(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10)))
      .toDF()
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    sqlContext.sql("SELECT firstName FROM people").collect()
    sqlContext.sql("SELECT firstName FROM people").show(5)

    val provinceCodeMap = sc.textFile("examples/spark_repl_demo/province_code_map.txt")
      .map(_.split("\t"))
      .map(p => ProvinceCodeMap2(p(0), p(1)))
      .toDF()
    provinceCodeMap.registerTempTable("provinceCodeMap")

    // Review map table data
    sqlContext.sql("SELECT srcProvinceCd, provinceCd FROM provinceCodeMap").show()

    // These tables can be joined in SQL like you do in a traditional RDBMS
    sqlContext.sql("SELECT p.firstName, p.lastName, p.province, pcm.provinceCd FROM people p LEFT OUTER JOIN provinceCodeMap pcm ON pcm.srcProvinceCd = p.province ORDER BY p.lastName").show(5)

    // How you can join the dataframes programatically
    val peopleWithProvinceCode = people.join(provinceCodeMap, people("province") === provinceCodeMap("srcProvinceCd"), "left_outer")

    // Take a look at how Spark executes this join
    peopleWithProvinceCode.explain()
    // Review the resulting dataframe schema
    peopleWithProvinceCode.schema
    // Look at the first 5 records
    peopleWithProvinceCode.show(5)

    // Wrap Standard.defaultInvalid rule with UDF so that it can be used in DataFrame
    val defaultInvalid = udf((srcVal: String, mappedVal: String) => Standard.defaultInvalid(srcVal, mappedVal).toString)

    // Execute the join add add a new column which is processed with our validate function to apply the invalid/default logic
    // Reduce results to those columns we wish to see and make it unique
    peopleWithProvinceCode
      .withColumn("final_province", defaultInvalid($"province", $"provinceCd"))
      .select("province", "provinceCd", "final_province")
      .distinct()
      .show()
  }
}


// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class Person(firstName: String, lastName: String, companyName: String, address: String, city: String, province: String, postal: String, phone1: String, phone2: String, email: String, web: String)

// Create a case class for a province code map table and load the data into a dataframe
case class ProvinceCodeMap2(provinceCd: String, srcProvinceCd: String)
