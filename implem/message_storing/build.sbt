ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.1.0"
libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "7.1.0"
resolvers += "confluent" at "https://packages.confluent.io/maven/"
lazy val root = (project in file("."))
  .settings(
    name := "message_storing"
  )
