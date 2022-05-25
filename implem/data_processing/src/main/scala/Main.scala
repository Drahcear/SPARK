import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Main {
  def main(args: Array[String]): Unit = {
    val input_blob_path = "topics/b/partition=0/"
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

    val schemaString = "{\"type\":\"record\"," +
      "\"name\":\"myrecord\"," +
      "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}"
    val schema: Schema = new Schema.Parser().parse(schemaString)
    val df = sparkSession.read
      .option("avroSchema", schema.toString)
      .format("avro")
      .load(s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/${input_blob_path}")
    df.show()
  }
}