package com.peak.taxi

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, date_format, hour, lit, monotonically_increasing_id}

import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val listOfFiles = ListUtils.getListOfFiles(inputDirectory)
    println(listOfFiles.head.toString)

    val spark = initSpark

    val parquetFileDFDropOff = getDataFrame(spark, listOfFiles.head.toString,
      "dropoff_datetime", "dropoff_taxizone_id")
    val parquetFileDFPickUp = getDataFrame(spark, listOfFiles.head.toString,
      "pickup_datetime", "pickup_taxizone_id")

    val parquetFileDF = parquetFileDFDropOff
      .union(parquetFileDFPickUp)
      .select(functions.concat(
        col("date"),
        lit(","),
        col("taxizone_id")).as("date_taxizone"))

    println(parquetFileDF.columns.mkString("Array(", ", ", ")"))

    val header = parquetFileDF.take(10)
    println(header.mkString("Array(", ", ", ")"))

    spark.stop()
  }

  def initSpark = {
    SparkSession
      .builder()
      .appName("PeakTaxi")
      .config("spark.master", "local")
      .getOrCreate()
  }

  private def getDataFrame(spark: SparkSession, file: String, dateTimeColumn: String, taxizoneColumn: String): DataFrame = {
    spark.read.parquet(file)
      .select(dateTimeColumn, taxizoneColumn)
      .withColumn(dateTimeColumn, date_format(col(dateTimeColumn), "yyyy-MM-dd HH"))
      .withColumnRenamed(dateTimeColumn, "date")
      .withColumnRenamed(taxizoneColumn, "taxizone_id")
  }
}
