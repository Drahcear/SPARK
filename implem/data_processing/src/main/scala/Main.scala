import Utils._
import org.apache.avro.Schema
import org.apache.hadoop.shaded.com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object Main {
  def main(args: Array[String]): Unit = {
    val input_blob_path = "topics/AvroTopic/test/truc/p/a/*.csv"
    val output_blob_path = "topics/test/new"
    val storageAccountName = "peaceland"
    val storageKeyValue = "T+Q1Ryvhx2MtghOSccN9oUeQwRM4I9GwiBy6L/6K2r2i98WkADBBa+KEA/t6aJO6Ii7nJU6+p4yPSgJppnG9YQ=="
    val containerName = "data"
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("test")
      .config(
        s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net",
        storageKeyValue)
      .getOrCreate()
    val schemaString = "{\n  \"name\": \"Message\",\n  \"type\": \"record\",\n  \"namespace\": \"com.acme.avro\",\n  \"fields\": [\n    {\n      \"name\": \"id\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"location\",\n      \"type\": \"string\"\n    },\n    {\n      \"name\": \"Date\",\n      \"type\": \"int\"\n    },\n    {\n      \"name\": \"Citizens\",\n      \"type\": {\n        \"type\": \"array\",\n        \"items\": {\n          \"name\": \"Citizen\",\n          \"type\": \"record\",\n          \"fields\": [\n            {\n              \"name\": \"Name\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"FirstName\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"Login\",\n              \"type\": \"string\"\n            },\n            {\n              \"name\": \"PeaceScore\",\n              \"type\": \"int\"\n            }\n          ]\n        }\n      }\n    },\n    {\n      \"name\": \"Words\",\n      \"type\": {\n        \"type\": \"array\",\n        \"items\": \"string\"\n      }\n    }\n  ]\n}"
    val schema: Schema = new Schema.Parser().parse(schemaString)
    val df = sparkSession.read
      .option("header", true)
      .csv(s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${input_blob_path}")

    val schema2 = StructType(Seq(
      StructField("id", StringType, true), StructField("location", StringType, true), StructField("Date", LongType, true), StructField("Citizens", StructType(Seq(
        StructField("Name", StringType, true), StructField("FirstName", StringType, true), StructField("Login", StringType, true), StructField("PeaceScore", IntegerType, true))
      ), true), StructField("Words", StructType(Seq(StructField("word", StringType, true))))
    ))

    val gson = new Gson()
    val list = df.select("value").collect.toList.map(x => x.toString).map(x => x.substring(1, x.length -1)).map(x => gson.fromJson(x, classOf[Message]))
    val df2 = sparkSession.createDataFrame(list)
    df2.show()
    //val df2 = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(df.select("value").collect().toSeq.take(1)), schema2)

    wordCount(df2)

    getScore(df2).show()

    worstPeaceScoreLocation(df2)

    getSummary(df2)

    getBadCitizen(df2)

    getGoodCitizen(df2)

    getTimestamp(df2).show()
  }
}