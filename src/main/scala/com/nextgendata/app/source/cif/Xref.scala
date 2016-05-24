package com.nextgendata.app.source.cif

import com.nextgendata.framework.Job
import org.apache.spark.sql.Dataset

/**
  * Created by Craig on 2016-05-13.
  */
object Xref {
  def getXref: Dataset[XrefRow] = {
    val file = Job.sc.textFile("examples/spark_repl_demo/cif_xref.txt")

    val sqlContext = Job.sqlContext
    // this is used to implicitly convert an RDD to a DataFrame or Dataset.
    import sqlContext.implicits._

    //Have to wrap this Spark code in a block so that the header val is only scoped to this call
    //not the entire class.  If header val was class level, then the closure would try to serialize
    //this entire class and fail since it contains non-serializable objects (SQL/SparkContext)
    val fileRdd = {
      val header = file.first()

      file
        .filter(line => line != header)
        .map(_.split("\t"))
        .map(p => XrefRow(p(0), p(1), p(2).toInt))
      //.toDS()
    }

    fileRdd.toDS
  }
}

case class XrefRow (XrefSystem: String,
                    XrefId: String,
                    CIFId: Int)