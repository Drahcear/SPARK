ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.3.3"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.1"
resolvers += "confluent" at "https://packages.confluent.io/maven/"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "7.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "data_processing"
  )
