package com.peak.taxi

import com.google.gson.Gson
import org.apache.spark.sql.functions.{count, desc}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.PrintWriter

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
      val peakTrips = taxiTripsDf
        .groupBy(Processor.date, Processor.taxizone_id)
        .agg(count("trip_id") as "count")
        .orderBy(desc("count"))
        .first
      val date = peakTrips.get(0).toString
      val taxizoneId = peakTrips.get(1).toString
      PeakHour(date, taxizoneId)
    })
  }

  private def writeJson(peakHourZoneId: PeakHour, outputDirectory: String) = {
    new PrintWriter(outputDirectory + "/result.json") {
      val gson = new Gson
      val jsonString: String = gson.toJson(peakHourZoneId)

      write(jsonString)
      close()
    }
  }

  private def writeParquet(inputDirectory: String, outputDirectory: String, peakHour: PeakHour): Unit = {
    SparkInstance.spark.time({

      val tripsInPeakHour = Processor
        .getTaxiTrips(inputDirectory)
        .filter(dropOffOrPickUpFilter(peakHour))

      tripsInPeakHour.write.mode(SaveMode.Overwrite).parquet(outputDirectory + "/trips.parquet")

    })
  }

  private def dropOffOrPickUpFilter(peakHour: PeakHour) = {
    row: Row => {
      val dropOffDate = row.getAs("dropoff_datetime").toString
      val dropOffTaxiZoneId = row.getAs("dropoff_taxizone_id").toString
      val dropOff = PeakHour(dropOffDate, dropOffTaxiZoneId)

      val pickUpDate = row.getAs("pickup_datetime").toString
      val pickUpTaxiZoneId = row.getAs("pickup_taxizone_id").toString
      val pickUp = PeakHour(pickUpDate, pickUpTaxiZoneId)

      dropOff.equals(peakHour) || pickUp.equals(peakHour)
    }
  }
}
