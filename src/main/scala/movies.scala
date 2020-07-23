import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object movies extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("MoviesOp")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //  moviesDf.show()

  //Exercise 1 : select any two column

  moviesDf.select("Director", "Distributor").show()

  import spark.implicits._

  moviesDf.select(
    moviesDf.col("Distributor"),
    column("IMDB_Rating"),
    $"IMDB_Rating", 'Production_Budget, expr("Running_Time_min"))
  //      .show()

  //calculate total profit

  val moviesRevenue = moviesDf.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    col("US_Gross") + col("Worldwide_Gross") as ("Total_Earning")
  )
  //      .show()

  moviesDf.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross  as Total_Gross"
  )
//    .show()

  moviesDf.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales")
  ).withColumn("Total_Revenue",    col("US_Gross") + col("Worldwide_Gross"))
//    .show()

  //select comedy movies with rating above 6

  moviesDf.select(col("Title"),col("IMDB_Rating")).filter(col("IMDB_Rating") > 6).show()

  moviesDf.select(col("Title"),col("IMDB_Rating")).where(col("IMDB_Rating") > 6).show()




}
