package com.peak.taxi

import com.google.gson.Gson
import com.peak.taxi.SparkUtils.initSpark
import org.apache.spark.sql.functions.{count, desc}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.PrintWriter

/**
 * todo:
 *  - decide how many cores to choose
 *  - test with all files
 *    * write result value in json file
 *    * find way to get the parquet files back from processor.
 *    idea 1:
 *      - select all columns from files
 *      - filter them by trip_id (the list in group by)
 *      - write the results in a parquet file
 *  - optimize: rdd?
 */
object Main {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val spark = initSpark

    val processor = new Processor();


    val taxiTripsDf = spark.time(processor.getTaxiTripsDataFrame(inputDirectory, spark))

    val peakHourZoneId = taxiTripsDf
      .groupBy(processor.date_taxizone)
      .agg(count("trip_id") as "count")
      .orderBy(desc("count"))
      .first()
      .get(0)
      .toString

    writePeakHourToJson(peakHourZoneId, outputDirectory)
    writeTripsToFiles(inputDirectory, outputDirectory, spark, processor, peakHourZoneId)

    spark.stop()

  }

  private def writePeakHourToJson(peakHourZoneId: String, outputDirectory: String) = {
    new PrintWriter(outputDirectory + "/result.json") {
      private val details: Array[String] = peakHourZoneId.split(",")
      val peakHour = details(0)
      val zone = details(1)
      val peakHourJson = new PeakHour(peakHour, zone)
      val gson = new Gson
      val jsonString = gson.toJson(peakHourJson)
      write(jsonString)
      close()
    }
  }

  private def writeTripsToFiles(inputDirectory: String, outputDirectory: String, spark: SparkSession, processor: Processor, peakHour: String) = {
    spark.time({

      val tripsInPeakHour = processor
        .getTaxiTripsAllFields(inputDirectory, spark)
        .filter(dropOffOrPickUpFilter(peakHour))

      tripsInPeakHour.write.parquet(outputDirectory)

    })
  }

  private def dropOffOrPickUpFilter(peakHour: String) = {
    row: Row => {
      val dropOff = row.getAs("dropoff_datetime").toString + "," + row.getAs("dropoff_taxizone_id").toString
      val pickUp = row.getAs("pickup_datetime").toString + "," + row.getAs("pickup_taxizone_id").toString

      dropOff.equals(peakHour) || pickUp.equals(peakHour)
    }
  }

  private def showParquetFile(parquetFileDF: DataFrame) = {
    println(parquetFileDF.columns.mkString("Array(", ", ", ")"))

    val header = parquetFileDF.take(10)
    println(header.mkString("Array(", ", ", ")"))
  }
}
