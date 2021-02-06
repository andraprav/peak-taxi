package com.peak.taxi

import com.peak.taxi.SparkUtils.initSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

/**
 * todo:
 *  - decide how many cores to choose
 *  - test with all files
 *  - write result value in json file
 *  - find way to get the parquet files back from processor.
 *    idea 1:
 *      - select all columns from files
 *      - filter them by trip_id (the list in group by)
 *      - write the results in a parquet file
 */
object Main {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val spark = initSpark

    val processor = new Processor();


    val taxiTripsDf = spark.time( processor.getTaxiTripsDataFrame(inputDirectory, spark))

    spark.time({
      println(taxiTripsDf.groupBy(processor.date_taxizone).count().orderBy(desc("count")).first())
    })//.show(false)

    spark.stop()
  }

  private def showParquetFile(parquetFileDF: DataFrame) = {
    println(parquetFileDF.columns.mkString("Array(", ", ", ")"))

    val header = parquetFileDF.take(10)
    println(header.mkString("Array(", ", ", ")"))
  }


}
