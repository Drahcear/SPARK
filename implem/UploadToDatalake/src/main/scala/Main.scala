import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Main {
  def main(args: Array[String]): Unit = {
    val input_blob_path = "topics/AvroTopic/partition=0/"
    val output_blob_path = "topics/test/new"
    val storageAccountName = "peaceland"
    val containerName = "data"
    //Consumer.poll("DroneStream")
    val storageKeyValue = "T+Q1Ryvhx2MtghOSccN9oUeQwRM4I9GwiBy6L/6K2r2i98WkADBBa+KEA/t6aJO6Ii7nJU6+p4yPSgJppnG9YQ=="
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SendToDL")
      .set(s"spark.hadoop.fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageKeyValue)

    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "DLSAVE",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("DroneStream")
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))
    //stream.cache()
    stream.map(record=>(record.value().toString)).foreachRDD(rdd =>  rdd.saveAsObjectFile(s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/jacky.txt"))
    ssc.start()
    ssc.awaitTermination()
  }
}