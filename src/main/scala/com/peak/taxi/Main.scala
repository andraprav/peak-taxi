package com.peak.taxi

object Main {
  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val listOfFiles = ListUtils.getListOfFiles(inputDirectory)
  }
}
