import MessageUtils.Message
import MessageUtils.Citizen

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
    infinite_produce(100)
  }

  def infinite_produce[T](n: Int) :Option[T] = n match {
    case 0 => None
    case n => {
      val msg = generateMessage()
      Producer.send(generateJsonFromMessage(msg._2), msg._1)
      infinite_produce(n - 1)
    }
  }

  def generateJsonFromMessage(message: Message): String = {
    write(message)(DefaultFormats)

  }

  def generateMessage() : (String, Message) = {
    val citizens = generateCitizens()
    val wordList = generateWordList() //recup the (word, score) List
    val peaceScore = wordList.map(x => x._2).sum / wordList.length
    val pos = (48.81568490222558 + scala.util.Random.nextDouble()) + "," + (2.363076 + scala.util.Random.nextDouble())
    val id = "drone_" + (scala.util.Random.nextInt(499) + 1)

    (id , Message( id , pos, System.currentTimeMillis, citizens.map(x => Citizen(x.Name, x.FirstName, x.Login, peaceScore)), wordList.map(x => x._1)))
  }

  def generateWordList() : List[(String,Int)] = {
    Random.shuffle(Source.fromFile("Discussions.txt").getLines().flatMap(_.split(" ")))
      .slice(0, scala.util.Random.nextInt(300) + 1).map(x => (x, scala.util.Random.nextInt(100))).toList
  }

  def generateCitizens() : List[Citizen] = {
    val lines = Source.fromFile("Citizens.txt").getLines()
    val citizen = MessageUtils.parseFromJson(lines)
    Random.shuffle(citizen).slice(0, scala.util.Random.nextInt(99) + 1)
  }

}