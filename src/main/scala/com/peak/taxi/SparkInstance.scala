package com.peak.taxi

import org.apache.spark.sql.SparkSession

object SparkInstance {
  val spark: SparkSession = initSpark

  def initSpark = {
    val sparkSession = SparkSession
      .builder()
      .appName("PeakTaxi")
      .config("spark.master", "local[*]")
      .getOrCreate
    sparkSession
      .sparkContext.setLogLevel("WARN")
    sparkSession
  }

  def stop(): Unit = {
    spark.stop()
  }
}
