// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
//case class Person(name: String, age: Int)
case class Person(firstName: String, lastName: String, companyName: String, address: String, city: String, province: String, postal: String, phone1: String, phone2: String, email: String, web: String)

// Create an RDD of People objects and register it as a table.
val people = sc.textFile("ca-500.txt").map(_.split("\t")).map(p => Person(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10))).toDF()
people.registerTempTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
sqlContext.sql("SELECT firstName FROM people").collect()
sqlContext.sql("SELECT firstName FROM people").show(5)

// Create a case class for a province code map table and load the data into a dataframe
case class ProvinceCodeMap(provinceCd: String, srcProvinceCd: String)

val provinceCodeMap = sc.textFile("province_code_map.txt").map(_.split("\t")).map(p => ProvinceCodeMap(p(0), p(1))).toDF()
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

// Functions used with DataFrames are wrapped in a UDF call, which is a higherorder function to convert from the SQL Column object context 
// needed at parsing, to getting values which is used during execution
val validate = udf((srcCol: String, trgCol: String) => {
  if (srcCol == null || srcCol.trim() == "") {
    "-99"
  }else if (trgCol == null) {
    "-1"
  }else{
    trgCol
  }
})

// Execute the join add add a new column which is processed with our validate function to apply the invalid/default logic
// Reduce results to those columns we wish to see and make it unique
(
peopleWithProvinceCode
  .withColumn("final_province", validate($"province", $"provinceCd"))
  .select("province", "provinceCd", "final_province")
  .distinct()
  .show()
)
