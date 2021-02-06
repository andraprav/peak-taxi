package com.peak.taxi

import com.peak.taxi.SparkUtils.initSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

object Main {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val spark = initSpark

    val processor = new Processor();

    val taxiTripsDf = processor.getDataFrame(inputDirectory, spark)

    taxiTripsDf.groupBy(processor.date_taxizone).count().orderBy(desc("count"))//.show(false)

    spark.stop()
  }

  private def showParquetFile(parquetFileDF: DataFrame) = {
    println(parquetFileDF.columns.mkString("Array(", ", ", ")"))

    val header = parquetFileDF.take(10)
    println(header.mkString("Array(", ", ", ")"))
  }


}
