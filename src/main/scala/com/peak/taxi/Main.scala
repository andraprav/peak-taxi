package com.peak.taxi

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val listOfFiles = ListUtils.getListOfFiles(inputDirectory)
    println(listOfFiles.head.toString)
    val spark = SparkSession
      .builder()
      .appName("PeakTaxi")
      .config("spark.master", "local")
      .getOrCreate()
    val parquetFileDF = spark.read.parquet(listOfFiles.head.toString)
    println(parquetFileDF.columns.mkString("Array(", ", ", ")"))

    spark.stop()
  }
}
