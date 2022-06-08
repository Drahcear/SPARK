import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords

import java.time.Duration
import scala.collection.JavaConverters._

object Consumer {
  def configConsumer(Topic : String) : KafkaConsumer[String, String] = {
    val consumerProps: Properties = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "AlertConsumer")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(List(Topic).asJava)
    consumer
  }

  def infiniteConsume[T](n: Int, consumer : KafkaConsumer[String, String]) :Option[T] = n match {
    case 0 => None
    case n => {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(5))
      records.asScala.foreach { record => Message.sendPeaceMaker(record.value()) }
      consumer.commitSync()
      infiniteConsume(n, consumer)
    }
  }

  def poll(Topic : String) : Unit = {
    val consumer = configConsumer(Topic)
    infiniteConsume(1, consumer)
    consumer.close()
  }
}
