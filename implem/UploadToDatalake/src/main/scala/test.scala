import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger

object test {
  def main(args: Array[String]): Unit = {
    val topic= "DroneStream"
    val bootstrapserver = "localhost:9092"

    val input_blob_path = "topics/AvroTopic/test/truc/p/a"
    val input_blob_path2 = "topics/AvroTopic/"
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
    val df : DataFrame = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapserver)
      .option("subscribe",topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val query = df .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("csv")
      .outputMode("append")
      .option("path", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${input_blob_path}")
      .option("checkpointLocation", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${input_blob_path2}")
      //.mode("overwrite")
      .option("header", "true")
      .option("failOnDataLoss", false)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    query.awaitTermination()
    //val myschema = Encoders.product[Message.Message].schema
    //df.select(from_json($"value".cast("string"),myschema).as("Data"))
      //.select("Data.*")

  }
}
