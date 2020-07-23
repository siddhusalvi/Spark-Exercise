import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNull {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession
     .builder()
     .appName("ManagingNulls")
     .master("local")
     .getOrCreate()

    val path = "src/main/resources/data/"

    val moviesDf = spark.read
      .option("inferSchema","true")
      .json(path + "movies.json")

//    moviesDf.show()

    //Select not null column
    moviesDf.select(
      col("Title"),
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating"),
      coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating")*10)
    )
//      .show()

    //Checking for nulls
    moviesDf.select("*").where(col("Rotten_Tomatoes_Rating")isNotNull)
//      .show()

    //Nulls when sorting
    moviesDf.orderBy(col("IMDB_Rating")desc_nulls_last)
      .select(col("Title"),col("IMDB_Rating"),col("Director"))
//      .show()

    //Removing nulls
    moviesDf.select(col("Title"),col("IMDB_Rating")).na.drop()
//      .show()

    //replace null
    moviesDf.na.fill(0,List("IMDB_Rating","Rotten_Tomatoes_Rating"))
      .select(col("Title"),col("IMDB_Rating"),col("Rotten_Tomatoes_Rating"))
//          .show()

    moviesDf.na.fill(Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    ))
//      .show()

    moviesDf.selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating * 10)", //choose not null value
      "nvl(Rotten_Tomatoes_Rating,IMDB_Rating * 10)",
      "nullif(Rotten_Tomatoes_Rating,IMDB_Rating * 10)",//return null if two values are equal else return first value
      "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating,Director)"//return not null value by order
    )
      .show()
  }
}
