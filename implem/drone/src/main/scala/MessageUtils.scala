import java.util.Properties
import com.google.gson._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

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

  def parseFromJson(lines:Iterator[String]):List[Citizen] = {
    val gson = new Gson()
    lines.map(line => gson.fromJson(line, classOf[Citizen])).toList
  }


  def createCitizenRecord(name: String, firstName : String, peacescore : Int, schema : String) : GenericRecord = {
    val citizenSchema : Schema = new Schema.Parser().parse(schema)
    val record = new GenericData.Record(citizenSchema)
    record.put("Name", name)
    record.put("FirstName", firstName)
    record.put("PeaceScore", peacescore)
    record
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer].getCanonicalName)
    props.put("schema.registry.url", "http://localhost:8081")
    val producer = new KafkaProducer[String, GenericRecord](props)
    val key = "key1"

    val userSchema = "{\n  \"name\": \"MyClass\",\n  \"type\": \"record\",\n  \"namespace\": \"com.acme.avro\",\n  \"fields\": [\n    {\n      \"name\": \"id\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"location\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"Date\",\n      \"type\": \"int\"\n    },\n    {\n      \"name\": \"Citizens\",\n      \"type\": {\n        \"type\": \"array\",\n        \"items\": {\n          \"name\": \"Citizens_record\",\n          \"type\": \"record\",\n          \"fields\": [\n            {\n              \"name\": \"Name\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"FirstName\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"Login\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"PeaceScore\",\n              \"type\": \"int\"\n            }\n          ]\n        }\n      }\n    },\n    {\n      \"name\": \"Words\",\n      \"type\": {\n        \"type\": \"array\",\n        \"items\": \"string\"\n      }\n    }\n  ]\n}"
    val citizenString = "{\n  \"name\": \"MyClass\",\n  \"type\": \"record\",\n  \"namespace\": \"com.acme.avro\",\n  \"fields\": [\n    {\n      \"name\": \"Name\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"FirstName\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"Login\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"PeaceScore\",\n      \"type\": \"int\"\n    }\n  ]\n}"

    val schema: Schema = new Schema.Parser().parse(userSchema)
    val words = List("ah", "bon", "maintenant", "c'est", "comme", "ca")

    val avroRecord = new GenericData.Record(schema)
    val listUser = List(("Grand", "Jacky", 100), ("Grand","Steven",100), ("Grand", "Richard", 100))
    val avroCitizens = listUser.map(x => createCitizenRecord(x._1,x._2, x._3, citizenString))

    avroRecord.put("id", 667)
    avroRecord.put("location", "dzdzdzedzd")
    avroRecord.put("Date", 21212)
    avroRecord.put("Citizens", avroCitizens)
    avroRecord.put("Words", words)

    //val writer = new SpecificDatumWriter[GenericRecord](schema)
    //val out = new ByteArrayOutputStream()
    //val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    //writer.write(avroRecord, encoder)
    //encoder.flush()
    //out.close()
    //val serializedBytes: Array[Byte] = out.toByteArray()
    val record = new ProducerRecord[String, GenericRecord]("b", key, avroRecord)

    producer.send(record)
    producer.flush()
    producer.close()
  }
}
