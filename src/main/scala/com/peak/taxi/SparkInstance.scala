package com.peak.taxi

import org.apache.spark.sql.SparkSession

object SparkInstance {
  val spark: SparkSession = initSpark

  def initSpark = {
    SparkSession
      .builder()
      .appName("PeakTaxi")
      .config("spark.master", "local[*]")
      .getOrCreate()
  }

  def stop(): Unit = {
    spark.stop()
  }
}
