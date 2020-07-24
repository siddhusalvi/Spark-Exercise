import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
object RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDs")
      .master("local")
      .getOrCreate()

    val path = "src/main/resources/data/"

    //RDD are first citizen of spark
    //RDD are distributed collection of JVM objects
    //We can partition,manipulate orders and operations in RDD
    //poor data processing support

    //spark context is required for RDD
    val sc =  spark.sparkContext

    // creating RDD using paralleling collection
    val numbers = 1 to 1000000
    val numbersRDD = sc.parallelize(numbers)

    //creating RDD reading from files
    case class Stockvalue(company:String,date:String,price:Double)

    def readStocks(filename:String) =
      Source.fromFile(filename)
        .getLines()
      .drop(1)                                  //drop header
        .map(line => line.split(","))
        .map(token => Stockvalue(token(0),token(1),token(2).toDouble))
        .toList

    val stocksRDD = readStocks(path + "stocks.csv")

    //creating RDD reading from files

    val stocksRDD2 = sc.textFile(path + "stocks.csv") //RDD of String
      .map(line => line.split(","))
      .filter(tokens => tokens(0).toUpperCase == tokens(0)) //filtering header
      .map(token => Stockvalue(token(0),token(1),token(2).toDouble))

    //Creating RDD from a DF

    val stocksDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(path + "stocks.csv")

    import spark.implicits._
    val stocksDS = stocksDF.as[Stockvalue]
    val stocksRDD3 = stocksDS.rdd

    val stocksRDD4 = stocksDF.rdd //RDD[ROW] loss of type information

    //RDD to DF
    val numbersDF =numbersRDD.toDF("numbers")

    //RDD to dataset
    val numbersDS = spark.createDataset(numbersRDD) //loss of typeinformation
    //dataset do not have type inforamation they only have rows

    /*
    rdd and dataset are distributed collections
    union and combine operation on rdd and dataset ,count, distinct, groupby, orderby

    RDD -partion control -repartition coalsec, optimization checkpoint cache storage
     */
  }
}
