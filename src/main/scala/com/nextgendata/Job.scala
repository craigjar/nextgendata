package com.nextgendata

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Craig on 2016-05-03.
  */
trait Job {
  //TODO implement a job trait, consider a Job context that can be shared?
  //TODO implement table counters with accumulators

  def init: Unit = {

  }

  def end: Unit = {

  }
}

object Job {
  private val _conf = new SparkConf().setAppName("MyLocalApp").setMaster("local[*]").set("spark.eventLog.enabled", "true")
  private var _sc : SparkContext = null

  private var _sqlContext : SQLContext = null

  def sc: SparkContext = {
    if (_sc == null) _sc = new SparkContext(_conf)
    _sc
  }

  def sqlContext: SQLContext = {
    if (_sqlContext == null) _sqlContext = new SQLContext(sc)
    _sqlContext
  }
}