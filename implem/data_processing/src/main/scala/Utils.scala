import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, udf}

object Utils {
  def wordCount(df: DataFrame): Unit = {
    //val badWords = List("students", "is", "for")
    println("Top 20 most used words :")
    val list = df.select("Words").collect()
      .map(x => x.getList(0).toArray())
      .flatMap(x => x.map(y => (y, 1)))
      .groupMapReduce(_._1)(_ => 1)(_+_)
      .toSeq.sortWith(_._2 > _._2)
      .take(20)
      //.filter(x => badWords.contains(x._1))
      .foreach(x => println(x))
  }

  def getLowestScore(df: DataFrame): Unit = {
    val tuple_udf = udf((l:Seq[String]) => l.map(x => (x, 1)))   // Give a value to the word
    val df2 = df.withColumn("Score", tuple_udf(col("Words")))
    val score_udf = udf((l:Seq[(String,Integer)]) => l.reduce((x, y) => ("score" ,x._2 + y._2))._2)  // Get de score
    val df3 = df2.withColumn("Score", score_udf(col("Score")))
    df3.orderBy(asc("Score")).show(1)   // Get the row with the lowest score
  }
}
