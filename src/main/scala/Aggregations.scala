import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("DFAggregations")
    .master("local")
    .getOrCreate()

  val data = "src/main/resources/data/"
  val moviesDF = spark.read
    .option("inferSchema","true")
    .json(data+"movies.json")

  moviesDF
    .show()

//  counting

    moviesDF.select(count(col("Major_Genre"))) //all value except null
    .show()

    moviesDF.selectExpr("Count(Major_Genre)")
    .show()

  moviesDF.select(count("*")) //count all and it include null
  .show()

  //count distinct
  moviesDF.select(countDistinct("Major_Genre"))
  .show()

  //approx counting for big data frame
  moviesDF.select(approx_count_distinct(col("Major_Genre")))
    .show()

  //min function
  moviesDF. select(min(col("IMDB_Rating")))
    .show()

  moviesDF.selectExpr("min(IMDB_Rating)")
    .show()

  //sum
  moviesDF.select(sum(col("Us_Gross")))
    .show()

  moviesDF.selectExpr("sum(Us_Gross)")

  //avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
    .show()

  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
    .show()

//  SD and mean

  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )
    .show()

  moviesDF.selectExpr(
    "mean(Rotten_Tomatoes_Rating)",
    "stddev(Rotten_Tomatoes_Rating)"
  )
    .show()

//  Grouping

  //select * from groupby major genre
  //groupby includes null
  moviesDF.groupBy("Major_Genre")
    .count()
    .show()

  moviesDF.groupBy("Major_Genre")
    .avg("IMDB_Rating")
    .show()

  moviesDF.groupBy("Major_Genre")
    .agg(
      count("*")as("movies"),
      avg("IMDB_Rating").as("AVGRAting")
    )
    .orderBy("AVGRAting")
    .show()

   //Exercise sum up all the profits of movies

  moviesDF.select(sum(col("US_Gross") + col("Worldwide_Gross")))
    .show()

//  Exercise count distinct directors

  moviesDF.select(countDistinct(col("Director")))
    .show()

//  Exercise calulate mean and standard deviation of US gross revenue for the movies

  moviesDF.select(
    mean(col("US_Gross")).as("Mean"),
    stddev(col("US_Gross")).as("SD")
  )
    .show()

//  Exercise average IMDB rating and US gross per directore

  moviesDF.groupBy("Director")
    .agg(avg("IMDB_Rating").as("Avg_Rating"),sum("US_Gross").as("Avg Gross"))
    .orderBy(col("Avg_Rating").desc_nulls_last)
//    .select(col("Director"),col("Rating"))
    .show()

}
