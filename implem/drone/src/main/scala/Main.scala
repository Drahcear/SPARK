import MessageUtils.Message
import MessageUtils.Citizen
import MessageUtils.parseFromJson

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import scala.io.Source
import scala.util.Random


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
//    println(scala.util.Random.nextInt(99) + 1)
  }

  def generateJsonFromMessage(message: Message): String = {
    write(message)(DefaultFormats)
  }

  def generateMessage() : Message = {
    val citizens = generateCitizens()
    println(citizens.length)
    Message("1111", "ici", 150000, citizens, List("Marre", "Intellij", "code", "Lib", "Nul"))
  }

  def generateWordList() : List[String] = {
    ???
  }

  def generateCitizens() : List[Citizen] = {
    val lines = Source.fromFile("Citizens.txt").getLines()
    val citizen = MessageUtils.parseFromJson(lines)
    Random.shuffle(citizen).slice(0, scala.util.Random.nextInt(99) + 1)
  }


}