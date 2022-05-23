import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.RecordMetadata

import java.time.Duration
import scala.collection.JavaConverters._

object MessageUtils {

  case class Message (
                       id : String,
                       location : String,
                       Date : Int,
                       Citizens : List[Citizen],
                       Words : List[String]
                     )

  case class Citizen(
                    Name : String,
                    FirstName : String,
                    Login : String,
                    PeaceScore : Int
                    )

  def main(args: Array[String]): Unit = {
    val topicName = "topic1"

    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](props)
    val key = "test1"
    val value = "Message 1"
    val record = new ProducerRecord[String, String](topicName, key, value)
    producer.send(record, (recordMetadata: RecordMetadata, exception: Exception) => {
      if (exception != null){
        exception.printStackTrace()
      }
      else {
        println(recordMetadata)
      }
    })
    producer.send(new ProducerRecord[String, String]("topic1", "test2", "Message 2"))
    producer.send(new ProducerRecord[String, String]("topic2", "test3", "Affiche toi"))

    producer.close()

    Consumer.poll()
  }

}
