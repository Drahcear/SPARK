import Utils._

import org.apache.hadoop.shaded.com.google.gson.Gson
import org.apache.spark.sql.{SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val inputBlobPath = "topics/Demo/*.csv"
    val storageAccountName = "peaceland"
    val storageKeyValue = "T+Q1Ryvhx2MtghOSccN9oUeQwRM4I9GwiBy6L/6K2r2i98WkADBBa+KEA/t6aJO6Ii7nJU6+p4yPSgJppnG9YQ=="
    val containerName = "data"
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