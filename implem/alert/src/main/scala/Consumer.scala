import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import java.time.Duration
import scala.collection.JavaConverters._

object Consumer {
  def poll() = {
    val topicName = "topic1"
    val consumer_props: Properties = new Properties()
    consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, "AlertConsumer")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumer_props)
    consumer.subscribe(List(topicName, "JsonTopic15").asJava)
    val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
    println(s"${records.count()} messages")
    records.asScala.foreach { record => println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}") }
    consumer.commitSync()
    consumer.close()
  }
}
