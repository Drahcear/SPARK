import java.util.Properties
import com.google.gson._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringDeserializer

import java.io.ByteArrayOutputStream

object MessageUtils {

  case class Message (
                       id : String,
                       location : String,
                       Date : Long,
                       Citizens : List[Citizen],
                       Words : List[String]
                     )

  case class Citizen(
                    Name : String,
                    FirstName : String,
                    Login : String,
                    PeaceScore : Int
                    )

  def parseFromJson(lines:Iterator[String]):List[Citizen] = {
    val gson = new Gson()
    lines.map(line => gson.fromJson(line, classOf[Citizen])).toList
  }
}
