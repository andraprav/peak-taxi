package com.peak.taxi

import org.apache.spark.sql.functions.{col, date_format, desc, lit}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Main {

  val trip_id = "trip_id"
  val date_taxizone = "date_taxizone"

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val listOfFiles = ListUtils.getListOfFiles(inputDirectory)
    println(listOfFiles.head.toString)

    val spark = initSpark

    val dropOffDataFrame = getDataFrame(spark, listOfFiles.head.toString,
      "dropoff_datetime", "dropoff_taxizone_id")
    val pickUpDataFrame = getDataFrame(spark, listOfFiles.head.toString,
      "pickup_datetime", "pickup_taxizone_id")

    val parquetFileDF = dropOffDataFrame
      .union(pickUpDataFrame)
      .filter(row => !row.anyNull)
      .select(getConcatColumn, col(trip_id))

    parquetFileDF.groupBy(date_taxizone).count().orderBy(desc("count"))

    spark.stop()
  }

  private def showParquetFile(parquetFileDF: DataFrame) = {
    println(parquetFileDF.columns.mkString("Array(", ", ", ")"))

    val header = parquetFileDF.take(10)
    println(header.mkString("Array(", ", ", ")"))
  }


  private def getConcatColumn = {
    functions.concat(
      col("date"),
      lit(","),
      col("taxizone_id")).as(date_taxizone)
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
      .select(dateTimeColumn, taxizoneColumn, trip_id)
      .withColumn(dateTimeColumn, date_format(col(dateTimeColumn), "yyyy-MM-dd HH"))
      .withColumnRenamed(dateTimeColumn, "date")
      .withColumnRenamed(taxizoneColumn, "taxizone_id")
  }
}
