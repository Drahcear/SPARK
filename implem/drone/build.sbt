ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "3.2.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.2.0"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.2.0"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "7.1.0"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.5.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.7"
resolvers += "confluent" at "https://packages.confluent.io/maven/"

lazy val root = (project in file("."))
  .settings(
    name := "drone"
  )
