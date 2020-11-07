//import org.apache.avro.io.Encoder
//import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
//import org.apache.spark.sql.functions._
//
//import scala.io.Source
//object RDD {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName("RDDs")
//      .master("local")
//      .getOrCreate()
//
//    val path = "src/main/resources/data/"
//
//    //RDD are first citizen of spark
//    //RDD are distributed collection of JVM objects
//    //We can partition,manipulate orders and operations in RDD
//    //poor data processing support
//
//    //spark context is required for RDD
//    val sc =  spark.sparkContext
//
//    // creating RDD using paralleling collection
//    val numbers = 1 to 1000000
//    val numbersRDD = sc.parallelize(numbers)
//
//    //creating RDD reading from files
//    case class Stockvalue(company:String,date:String,price:Double)
//
//    def readStocks(filename:String) =
//      Source.fromFile(filename)
//        .getLines()
//      .drop(1)                                  //drop header
//        .map(line => line.split(","))
//        .map(token => Stockvalue(token(0),token(1),token(2).toDouble))
//        .toList
//
//    val stocksRDD = sc.parallelize(readStocks(path + "stocks.csv"))
//
//    //creating RDD reading from files
//
////    val stocksRDD2 = sc.textFile(path + "stocks.csv") //RDD of String
////      .map(line => line.split(","))
////      .filter(tokens => tokens(0).toUpperCase == tokens(0)) //filtering header
////      .map(token => Stockvalue(token(0),token(1),token(2).toDouble))
////
//
//    //Creating RDD from a DF
//
//    val stocksDF = spark.read
//      .option("header","true")
//      .option("inferSchema","true")
//      .csv(path + "stocks.csv")
//
//    import spark.implicits._
////    val stocksDS = stocksDF.as[Stockvalue]
////    val stocksRDD3 = stocksDS.rdd
//
//    val stocksRDD4 = stocksDF.rdd //RDD[ROW] loss of type information
//
//    //RDD to DF
//    val numbersDF =numbersRDD.toDF("numbers")
//
//    //RDD to dataset
//    val numbersDS = spark.createDataset(numbersRDD) //loss of typeinformation
//    //dataset do not have type inforamation they only have rows
//
//    /*
//    rdd and dataset are distributed collections
//    union and combine operation on rdd and dataset ,count, distinct, groupby, orderby
//
//    RDD -partion control -repartition coalsec, optimization checkpoint cache storage
//     */
//
//    //Transformations on RDD
//    val msftRDD =  stocksRDD.filter(_.company == "MSFT")
//
//    //counting
//    val msCount = msftRDD.count()
//
//    //distinct
//    val comanyNamesRDD = stocksRDD.map(_.company)
//      .distinct()
//
//    //min and max
//    implicit val stockOrdering:Ordering[Stockvalue] = Ordering.fromLessThan((sa,sb) => sa.price < sb.price )
//    val min_microsoft = msftRDD.min()
//
//    //reducing
//    numbersRDD.reduce(_ + _)
//
//    //grouping
//    val groupStocksRDD = stocksRDD.groupBy(_.company)
//
//    //Partitioning
//    val repartitionStocksRDD = stocksRDD.repartition(30)
//
//    //Repartitioning is the expensive operation
//    //    repartitionStocksRDD.toDF.write.mode(SaveMode.Overwrite).parquet(path+"repartitiondata")
//    //  Common partition size should be  100
//
//    //coalsec it reduces number of partions
//    // it does not necessaryly involved shuffling
//    val coalsecedRDD = repartitionStocksRDD.coalesce(15)
//
//    //Exercise
//
//    //Read movies json as RDD
//    case class Movies(title:String, genre:String, rating:Double)
//
//    val moviesDF = spark.read
//      .option("inferSchema","true")
//      .json(path + "movies.json")
//
//    import spark.implicits
//    val moviesRDD = moviesDF.
//      select(col("Title")as("title"),col("Major_Genre")as("genre"),col("IMDB_Rating")as("rating "))
//      .where(col("genre").isNotNull and col("rating").isNotNull)
//      .as[Movies]
//      .rdd
//
//    //Show the distinct genre as RDD
//    val genreRDD = moviesRDD.map(_.genre).distinct()
//
//    //select all movies in drama genre with rating > 6
//    val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "drama" && movie.rating > 6)
//
//    moviesRDD.toDF().show()
//    genreRDD.toDF().show()
//    goodDramasRDD.toDF().show()
//
//    case class  GenreAvgRating(genre:String, rating: Double)
//    //Average rating by genre
//    val avgRatingbyGenereRDD = moviesRDD.groupBy(_.genre)
//      .map{
//        case(genre,movies) => GenreAvgRating(genre, movies.map(_.rating).sum/movies.size)
//      }
//
//    avgRatingbyGenereRDD.toDF().show()
//    moviesDF.toDF().groupBy(col("genre")).avg("rating").show()
//
//  }
//}
