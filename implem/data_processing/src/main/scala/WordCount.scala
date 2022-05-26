import org.apache.spark.sql.DataFrame

object WordCount {
  def wordCount(df: DataFrame) {
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
}
