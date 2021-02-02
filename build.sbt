import Dependencies.Libraries

name := "peak-taxi"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  Libraries.sparkCore,
  Libraries.sparkSql,
  Libraries.sparkStreaming
)
