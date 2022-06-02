ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.3.3"



libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"




lazy val root = (project in file("."))
  .settings(
    name := "UploadToDatalake"
  )
