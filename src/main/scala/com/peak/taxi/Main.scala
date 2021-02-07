package com.peak.taxi

import com.google.gson.Gson
import org.apache.spark.sql.functions.{count, desc}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.PrintWriter

/**
 * todo:
 *  - decide how many cores to use
 *  - test with all files
 */
object Main {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val taxiTripsDf = Processor.getTaxiTripsDataFrame(inputDirectory)

    val peakHourZoneId = getPeakHour(taxiTripsDf)

    writeJson(peakHourZoneId, outputDirectory)
    writeParquet(inputDirectory, outputDirectory, peakHourZoneId)

    SparkInstance.spark.stop()
  }

  private def getPeakHour(taxiTripsDf: DataFrame) = {
    SparkInstance.spark.time({
      taxiTripsDf
        .groupBy(Processor.date_taxizone)
        .agg(count("trip_id") as "count")
        .orderBy(desc("count"))
        .first()
        .get(0)
        .toString
    })
  }

  private def writeJson(peakHourZoneId: String, outputDirectory: String) = {
    new PrintWriter(outputDirectory + "/result.json") {
      private val details: Array[String] = peakHourZoneId.split(",")
      val peakHour: String = details(0)
      val zone: String = details(1)

      val peakHourJson = new PeakHour(peakHour, zone)
      val gson = new Gson
      val jsonString: String = gson.toJson(peakHourJson)

      write(jsonString)
      close()
    }
  }

  private def writeParquet(inputDirectory: String, outputDirectory: String, peakHour: String): Unit = {
    SparkInstance.spark.time({

      val tripsInPeakHour = Processor
        .getTaxiTrips(inputDirectory)
        .filter(dropOffOrPickUpFilter(peakHour))

      tripsInPeakHour.write.mode(SaveMode.Overwrite).parquet(outputDirectory + "/trips.parquet")

    })
  }

  private def dropOffOrPickUpFilter(peakHour: String) = {
    row: Row => {
      val dropOff = row.getAs("dropoff_datetime").toString + "," + row.getAs("dropoff_taxizone_id").toString
      val pickUp = row.getAs("pickup_datetime").toString + "," + row.getAs("pickup_taxizone_id").toString

      dropOff.equals(peakHour) || pickUp.equals(peakHour)
    }
  }
}
