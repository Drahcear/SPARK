import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File


object StreamProcessing{
    def main(args: Array[String]): Unit = {
        val conf = ConfigFactory.parseFile(new File("src/main/ressources/application.conf"))
        val topic = conf.getString("app.topic")
        val bootstrapServer = "localhost:9092"

        val inputBlobPath = conf.getString("app.inputBlobPath")
        val inputSparkCheckpoint = conf.getString("app.inputSparkCheckpoint")
        val storageAccountName = conf.getString("app.storageAccountName")
        val storageKeyValue = conf.getString("app.key")
        val containerName = conf.getString("app.containerName")


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
          .trigger(Trigger.ProcessingTime("30 seconds"))
          .start()

        query.awaitTermination()
    }
}
