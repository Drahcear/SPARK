ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.3.3"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "data_processing"
  )
