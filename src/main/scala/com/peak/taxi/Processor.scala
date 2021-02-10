package com.peak.taxi

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, functions}

import java.sql.Timestamp
import java.util.Calendar

object Processor {
  val date = "date"
  val taxizone_id = "taxizone_id"
  private val trip_id = "trip_id"
  private val pickup_datetime = "pickup_datetime"
  private val dropoff_datetime = "dropoff_datetime"
  private val pickup_taxizone_id = "pickup_taxizone_id"
  private val dropoff_taxizone_id = "dropoff_taxizone_id"
  private val truncateDateUdf = functions.udf[Timestamp, Timestamp](truncateDate)

  /**
   *
   * @return a data frame containing 2 columns:
   *         date_taxizone -> separated by comma
   *         there are both pickups and dropoffs
   *         the date is stripped by minute and seconds, format: yyyy-MM-dd HH
   *         +----------------+-------+
   *         |date_taxizone   |trip_id|
   *         +----------------+-------+
   *         |2009-01-25 01,79|5263   |
   *         ............
   */
  def getTaxiTripsDataFrame(inputDirectory: String): DataFrame = {

    val taxiTrips = getTaxiTripsToProcess(inputDirectory)

    val pickups = renameColumns(taxiTrips, pickup_datetime, pickup_taxizone_id)
    val dropOffs = renameColumns(taxiTrips, dropoff_datetime, dropoff_taxizone_id)

    pickups.union(dropOffs)
  }

  private def renameColumns(taxiTrips: DataFrame, pickup_datetime: String, pickup_taxizone_id: String) = {
    taxiTrips.select(pickup_datetime, pickup_taxizone_id, trip_id)
      .withColumnRenamed(pickup_datetime, date)
      .withColumnRenamed(pickup_taxizone_id, taxizone_id)
  }

  private def getTaxiTripsToProcess(inputDirectory: String) = {
    SparkInstance.spark.read.load(inputDirectory ++ "/*")
      .select(pickup_datetime, pickup_taxizone_id, dropoff_datetime, dropoff_taxizone_id, trip_id)
      .filter(row => !row.anyNull)
      .withColumn(dropoff_datetime, truncateDateUdf(col(dropoff_datetime)))
      .withColumn(pickup_datetime, truncateDateUdf(col(pickup_datetime)))
  }

  def getTaxiTrips(inputDirectory: String): DataFrame = {
    val dateFormat = "yyyy-MM-dd HH"

    SparkInstance.spark.read.load(inputDirectory ++ "/*")
      .filter(row => row.getAs("dropoff_datetime") != null)
      .filter(row => row.getAs("pickup_datetime") != null)
      .filter(row => row.getAs("pickup_taxizone_id") != null)
      .filter(row => row.getAs("dropoff_taxizone_id") != null)
      .withColumn(dropoff_datetime, truncateDateUdf(col(dropoff_datetime)))
      .withColumn(pickup_datetime, truncateDateUdf(col(pickup_datetime)))
  }

  private def truncateDate(date: Timestamp) = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    Timestamp.from(cal.toInstant)
  }
}
