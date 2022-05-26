ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"
lazy val root = (project in file("."))
  .settings(
    name := "message_storing"
  )
