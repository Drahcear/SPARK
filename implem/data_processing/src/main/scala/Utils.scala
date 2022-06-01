import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, asc, col, date_format, desc, explode, from_unixtime, lit, udf}

object Utils {
  case class Citizen(
                      Name : String,
                      FirstName : String,
                      Login : String,
                      PeaceScore : Int
                    )
  def wordCount(df: DataFrame): Unit = {
    println("Top 20 most used words :")
    val list = df.select("Words")
      .collect()
      .map(x => x.getList(0).toArray())
      .flatMap(x => x.map(y => (y, 1)))
      .groupMapReduce(_._1)(_ => 1)(_+_)
      .toSeq.sortWith(_._2 > _._2)
      .take(20)
      .foreach(x => println(x))
  }

  def getScore(df: DataFrame): DataFrame = {
    val length_udf = udf((l:Seq[Citizen]) => l.map(x => (x, 1)))   // Give a value to the word
    val df2 = df.withColumn("Length", length_udf(col("Citizens")))
    val reduce_udf = udf((l:Seq[(Citizen,Integer)]) => l.reduce((x, y) => (x._1 ,x._2 + y._2))._2)  // Get de score
    val df3 = df2.withColumn("Length", reduce_udf(col("Length")))
    val tuple_udf = udf((l:Seq[Citizen]) => l.map(x => (x.Login, x.PeaceScore)))
    val df4 = df3.withColumn("PeaceScore", tuple_udf(col("Citizens")))
    val score_udf2 = udf((l:Seq[(String, Integer)]) => l.reduce((x, y) => ("Score", x._2 + y._2))._2)
    val df5 = df4.withColumn("PeaceScore", score_udf2(col("PeaceScore"))/col("Length"))
    df5
  }

  def getLowestScore(df: DataFrame): Unit = {
    val df1 = getScore(df)
    df1.orderBy(asc("PeaceScore"))
      .show(20)   // Get the row with the lowest score
  }

  def getSummary(df: DataFrame): Unit = {
    val df1 = getScore(df)
    df1.describe("PeaceScore")
      .show()
  }

  def getBadLocation(df: DataFrame, seuil: Int): Unit = {
    val df1 = getScore(df)
    // Duplique des lignes (à enlever pour plus tard)
    //val dup_df = df1.withColumn("new_col", explode(array((1 until 4).map(lit): _*))).selectExpr(df.columns: _*)
    val df_sc = getScore(df1)
    df_sc.where(col("PeaceScore") <= seuil)
      .groupBy("location")
      .count()
      .sort(desc("count"))
      .show()
  }

  def getTimestamp(df: DataFrame) : DataFrame = {
    df.withColumn("Timestamp", from_unixtime(col("Date"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("week_day_abb", date_format(col("Timestamp"), "E"))
  }
}
