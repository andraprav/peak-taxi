import sbt._

object Dependencies {

  object Versions {
    val spark = "2.3.2"
  }

  object Libraries {
    lazy val sparkCore = "org.apache.spark"      %% "spark-core"      % Versions.spark
    lazy val sparkSql =  "org.apache.spark"      %% "spark-sql"       % Versions.spark
    lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % Versions.spark
  }
}
