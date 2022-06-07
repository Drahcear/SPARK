import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object StreamProcessing{
    def main(args: Array[String]): Unit = {
        val topic = "DroneStream"
        val bootstrapServer = "localhost:9092"

        val inputBlobPath = "topics/Demo"
        val inputSparkCheckpoint = "topics/Checkpoints"
        val storageAccountName = "peaceland"
        val storageKeyValue = "T+Q1Ryvhx2MtghOSccN9oUeQwRM4I9GwiBy6L/6K2r2i98WkADBBa+KEA/t6aJO6Ii7nJU6+p4yPSgJppnG9YQ=="
        val containerName = "data"


        val sparkSession = SparkSession.builder()
          .master("local")
          .appName("uploadToDataLakeAzure")
          .config(
              s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net",
              storageKeyValue)
          .getOrCreate()
        val df: DataFrame = sparkSession.readStream.format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServer)
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load()

        val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .writeStream
          .format("csv")
          .outputMode("append")
          .option("path", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${inputBlobPath}")
          .option("checkpointLocation", s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${inputSparkCheckpoint}")
          .option("header", "true")
          .option("failOnDataLoss", false)
          .trigger(Trigger.ProcessingTime("15 seconds"))
          .start()

        query.awaitTermination()
    }
}
