package com.nextgendata.framework

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Craig on 2016-05-03.
  */
trait Job {
  //TODO implement a job trait, consider a Job context that can be shared?
  //TODO implement table counters with accumulators

  def init(): Unit  = {

  }

  def end(): Unit = {

  }
}

object Job {
  // use lazy to achieve thread-safety (atomic initialization) and provide effective global immutable data.
  // As side benefit, avoids using null for initialization.
  private lazy val initConf = new SparkConf()
    .setAppName("MyLocalApp")
    .setMaster("local[*]")
    //.set("spark.eventLog.enabled", "true") //Temporarily removing event logging to fix Jenkins build

  private lazy val _sc = new SparkContext(initConf)

  private lazy val _sqlContext = {
    val x = new SQLContext(_sc)
    //Reducing the number of shuffle partitions from 200 (default) to 2 for performance
    x.setConf( "spark.sql.shuffle.partitions", "2")
    x
  }

  def sc: SparkContext = _sc

  def sqlContext: SQLContext = _sqlContext

}