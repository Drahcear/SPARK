ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
libraryDependencies += "org.apache.kafka" %% "kafka" % "3.2.0"
lazy val root = (project in file("."))
  .settings(
    name := "alert"
  )
