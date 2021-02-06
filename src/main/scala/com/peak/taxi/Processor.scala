package com.peak.taxi

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, date_format, lit}

class Processor {
  val trip_id = "trip_id"
  val date_taxizone = "date_taxizone"

  private val pickup_datetime = "pickup_datetime"
  private val dropoff_datetime = "dropoff_datetime"

  private val pickup_taxizone_id = "pickup_taxizone_id"
  private val dropoff_taxizone_id = "dropoff_taxizone_id"

  val date = "date"
  val taxizone_id = "taxizone_id"

  /**
   *
   * @param inputDirectory
   * @param spark
   * @return a data frame containing 2 columns:
   *  date_taxizone -> separated by comma
   *  there are both pickups and dropoffs
   *  the date is stripped by minute and seconds, format: yyyy-MM-dd HH
   *  +----------------+-------+
      |date_taxizone   |trip_id|
      +----------------+-------+
      |2009-01-25 01,79|5263   |
      ............
   */
  def getDataFrame(inputDirectory: String, spark: SparkSession) = {

    val taxiTrips = getTaxiTrips(inputDirectory, spark)

    val pickups = renameColumns(taxiTrips, pickup_datetime, pickup_taxizone_id)
    val dropOffs = renameColumns(taxiTrips, dropoff_datetime, dropoff_taxizone_id)

    val allTrips = pickups.union(dropOffs)
    allTrips.select(dateTaxizoneColumn, col(trip_id))
  }

  private def renameColumns(taxiTrips: DataFrame, pickup_datetime: String, pickup_taxizone_id: String) = {
    taxiTrips.select(pickup_datetime, pickup_taxizone_id, trip_id)
      .withColumnRenamed(pickup_datetime, date)
      .withColumnRenamed(pickup_taxizone_id, taxizone_id)
  }

  private def getTaxiTrips(inputDirectory: String, spark: SparkSession) = {
    val dateFormat = "yyyy-MM-dd HH"

    spark.read.load(inputDirectory ++ "/*")
      .select(pickup_datetime, pickup_taxizone_id, dropoff_datetime, dropoff_taxizone_id, trip_id)
      .filter(row => !row.anyNull)
      .withColumn(dropoff_datetime, date_format(col(dropoff_datetime), dateFormat))
      .withColumn(pickup_datetime, date_format(col(pickup_datetime), dateFormat))
  }

  private def dateTaxizoneColumn = {
    functions.concat(
      col(date),
      lit(","),
      col(taxizone_id)).as(date_taxizone)
  }
}
