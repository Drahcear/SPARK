import MessageUtils.{Citizen, Message, send}

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write


// kafka import
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients
import java.util.Properties


object Main {

  def main(args: Array[String]): Unit = {
    println("Hello world!")
    println(generateJsonFromMessage(generateMessage()))
    send("Drh", generateJsonFromMessage(generateMessage()))
  }

  def generateJsonFromMessage(message: Message): String = {
    write(message)(DefaultFormats)
  }

  def generateMessage() : Message = {
    val citizen1 = Citizen("Tien", "Steven", "steven.tien", 40 )
    Message("1111", "ici", 150000, List(citizen1), List("Marre", "Intellij", "code", "Lib", "Nul"))
  }

  def generateWordList() : List[String] = {
    ???
  }

  def generateCitizen() : Citizen = {
    ???

  }




}