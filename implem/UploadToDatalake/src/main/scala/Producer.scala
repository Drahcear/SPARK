import java.util.Properties
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object Producer {
  def createCitizenRecord(name: String, firstName : String, login : String ,peacescore : Int, schema : Schema) : GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("Name", name)
    record.put("FirstName", firstName)
    record.put("PeaceScore", peacescore)
    record.put("Login", login)
    record
  }

  def Send(message : Message.Message, key: String): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://localhost:8081")
    val producer = new KafkaProducer[String, GenericRecord](props)

    val userSchema = "{\n  \"name\": \"Message\",\n  \"type\": \"record\",\n  \"namespace\": \"com.acme.avro\",\n  \"fields\": [\n    {\n      \"name\": \"id\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"location\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"Date\",\n      \"type\": \"int\"\n    },\n    {\n      \"name\": \"Citizens\",\n      \"type\": {\n        \"type\": \"array\",\n        \"items\": {\n          \"name\": \"Citizen\",\n          \"type\": \"record\",\n          \"fields\": [\n            {\n              \"name\": \"Name\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"FirstName\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"Login\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"PeaceScore\",\n              \"type\": \"int\"\n            }\n          ]\n        }\n      }\n    },\n    {\n      \"name\": \"Words\",\n      \"type\": {\n        \"type\": \"array\",\n        \"items\": \"string\"\n      }\n    }\n  ]\n}"
    val schema: Schema = new Schema.Parser().parse(userSchema)
    val arraySchema = Schema.createArray(schema.getField("Citizens").schema().getElementType())
    val wordsSchema = Schema.createArray(schema.getField("Words").schema().getElementType())
    val avroRecord = new GenericData.Record(schema)
    val citizenRecord = new GenericData.Record(schema.getField("Citizens").schema().getElementType())
    avroRecord.put("id", message.id)
    avroRecord.put("location", message.location)
    avroRecord.put("Date", message.Date)
    val GenericArray = new GenericData.Array[GenericRecord](message.Citizens.size, arraySchema)
    val GenericArrayWords = new GenericData.Array[String](message.Citizens.size, wordsSchema)
    message.Words.foreach(x => GenericArrayWords.add(x))
    message.Citizens.map(x=> createCitizenRecord(x.Name, x.FirstName, x.Login, x.PeaceScore, schema.getField("Citizens").schema().getElementType())).foreach(y => GenericArray.add(y))
    avroRecord.put("Citizens", GenericArray)
    avroRecord.put("Words", GenericArrayWords)
    val record = new ProducerRecord[String, GenericRecord]("AvroTopic", key, avroRecord)

    producer.send(record)
    producer.flush()
    producer.close()
  }
}
