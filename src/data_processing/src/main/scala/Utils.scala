import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, date_format, desc, from_unixtime, udf}

object Utils {

  case class Message (
                       id : String,
                       location : String,
                       Date : Long,
                       Citizens : Array[Citizen],
                       Words : Array[String]
                     )

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
      .filter(x => x._1.toString.length() > 3)
      .groupMapReduce(_._1)(_ => 1)(_+_)
      .toSeq.sortWith(_._2 > _._2)
      .take(20)
      .foreach(x => println(x))
  }

  def getScore(df: DataFrame): DataFrame = {
    val lengthUdf = udf((l:Seq[Citizen]) => l.map(x => (x, 1)))   // Give a value to the word
    val reduceUdf = udf((l:Seq[(Citizen,Integer)]) => l.reduce((x, y) => (x._1 ,x._2 + y._2))._2)  // Get de score
    val tupleUdf = udf((l:Seq[Citizen]) => l.map(x => (x.Login, x.PeaceScore)))
    val scoreUdf = udf((l:Seq[(String, Integer)]) => l.reduce((x, y) => ("Score", x._2 + y._2))._2)

    df.withColumn("Length", lengthUdf(col("Citizens")))
      .withColumn("Length", reduceUdf(col("Length")))
      .withColumn("PeaceScore", tupleUdf(col("Citizens")))
      .withColumn("PeaceScore", scoreUdf(col("PeaceScore"))/col("Length"))
  }

  def worstPeaceScoreLocation(df: DataFrame): Unit = {
    val df1 = getScore(df)
    df1.withColumn("Timestamp", from_unixtime(col("Date"), "yyyy-MM-dd HH:mm:ss"))
      .orderBy(asc("PeaceScore"))
      .select("location", "Timestamp", "PeaceScore")
      .show(20)   // Get the row with the lowest score
  }

  def getSummary(df: DataFrame): Unit = {
    val df1 = getScore(df)
    df1.describe("PeaceScore")
      .show()
  }

  def getBadCitizen(df: DataFrame): Unit = {
    val df1 = getScore(df)
    val dfSC = getScore(df1)
    val tupleCitizen = udf((l:Seq[Citizen]) => l.map(x => (x.Login, x.PeaceScore)))
    val worstCitizen = udf((l:Seq[(String, Integer)]) => Array(l.minBy(_._2)))
    val score = udf((l:Seq[(String, Integer)]) => l.map(x => x._2))
    dfSC.withColumn("Worst_citizen", tupleCitizen(col("Citizens")))
      .withColumn("Worst_citizen", worstCitizen(col("Worst_citizen")))
      .withColumn("Score", score(col("Worst_citizen")))
      .orderBy(asc("Score"))
      .select("id", "location", "Date", "Worst_citizen", "Score")
      .show()
  }

  def getGoodCitizen(df: DataFrame): Unit = {
    val df1 = getScore(df)
    val dfSC = getScore(df1)
    val tupleCitizen = udf((l:Seq[Citizen]) => l.map(x => (x.Login, x.PeaceScore)))
    val bestCitizen = udf((l:Seq[(String, Integer)]) => Array(l.maxBy(_._2)))
    val score = udf((l:Seq[(String, Integer)]) => l.map(x => x._2))
    dfSC.withColumn("Best_citizen", tupleCitizen(col("Citizens")))
      .withColumn("Best_citizen", bestCitizen(col("Best_citizen")))
      .withColumn("Score", score(col("Best_citizen")))
      .orderBy(desc("Score"))
      .select("id", "location", "Date", "Best_citizen", "Score")
      .show()
  }

  def getTimestamp(df: DataFrame) : DataFrame = {
    val df1 = getScore(df)
    df1.withColumn("Timestamp", from_unixtime(col("Date"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("week_day", date_format(col("Timestamp"), "E"))
      .where(col("PeaceScore") < 50).groupBy("week_day").count()
  }
}
