import Utils._
import org.apache.hadoop.shaded.com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

object Main {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File("src/main/ressources/application.conf"))
    val bootstrapServer = "localhost:9092"

    val inputBlobPath = conf.getString("app.inputBlobPath")
    
    val storageAccountName = conf.getString("app.storageAccountName")
    val storageKeyValue = conf.getString("app.key")
    val containerName = conf.getString("app.containerName")    
    
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("DataProcessing")
      .config(
        s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net",
        storageKeyValue)
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val df = sparkSession.read
      .option("header", true)
      .csv(s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${inputBlobPath}")


    val gson = new Gson()
    val list = df.select("value")
      .collect
      .toList
      .map(x => x.toString)
      .map(x => x.substring(1, x.length -1))
      .map(x => gson.fromJson(x, classOf[Message]))
    val df2 = sparkSession.createDataFrame(list)
    df2.show()

    wordCount(df2)

    getScore(df2).show()

    worstPeaceScoreLocation(df2)

    getSummary(df2)

    getBadCitizen(df2)

    getGoodCitizen(df2)

    getTimestamp(df2).show()
  }
}