import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords

import java.time.Duration
import scala.collection.JavaConverters._
object Consumer {
  def config_Consumer(Topic : String) : KafkaConsumer[String, String] = {
    val consumer_props: Properties = new Properties()
    consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, "DataLakeConsumer")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumer_props)
    consumer.subscribe(List(Topic).asJava)
    consumer
  }

  def infinite_consume[T](n: Int, consumer : KafkaConsumer[String, String]) :Option[T] = n match {
    case 0 => None
    case n => {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(5))
      records.asScala.foreach { record => Producer.Send(Message.parseFromJson(record.value()), record.key()) }
      consumer.commitSync()
      infinite_consume(n, consumer)
    }
  }

  def poll(Topic : String) : Unit = {
    val consumer = config_Consumer(Topic)
    infinite_consume(1, consumer)
    consumer.close()
  }
}