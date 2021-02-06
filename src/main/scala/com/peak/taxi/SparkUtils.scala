package com.peak.taxi

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def initSpark = {
    SparkSession
      .builder()
      .appName("PeakTaxi")
      .config("spark.master", "local")
      .getOrCreate()
  }
}
