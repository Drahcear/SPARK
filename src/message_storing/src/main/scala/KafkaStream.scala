import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.Printed

import java.time.Duration
import java.util.Properties
object DroneSteam {

def initStream() {
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "DroneMessage")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder
  val textLines: KStream[String, String] = builder.stream[String, String]("DroneStream")
  val sysout = Printed
    .toSysOut[String, String]
    .withLabel("a")
  textLines.print(sysout)
  textLines.to("StoredMessage")
  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()
  println("ready")
  Thread.sleep(1000000000) // Sans Thread sleep, sbt run termine instantanément le programme alors que intellij fonctionne sans.

}
}

