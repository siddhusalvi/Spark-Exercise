import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("CommonTypes")
    .master("local")
    .getOrCreate()

  val data = "src/main/resources/data/"
  val moviesDF = spark.read
      .option("inferSchema","true")
    .json(data + "movies.json")

  moviesDF.show()

    //modify and add data to dataframe

  //adding a plance value
  moviesDF.select(col("Title"),lit(47)as("Plain_value"))
    .show()

  //Booleans
  moviesDF.select(col("Title")).where(col("Major_Genre") === "Drama")
    .show()

  val dramaFilter = col("Major_Genre")equalTo("Drama")
  val goodRatingFilter = col("IMDB_Rating") > 8.0
  val preferedFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title","Director").where(preferedFilter)
    .show()

  moviesDF.select(col("Title"),preferedFilter.as("Good_Movie"))
    .show()

  //Shows noly good moview
  //filter on colums
  moviesDF.select(col("Title"),preferedFilter.as("Good_Movie"))
    .where(col("Good_Movie"))
    .show()

  //Numbers FloatTypes,IntegerTypes,DoubleType

  //Calculating Average rating

  //Math operators
  val moviesAvgRatingDF = moviesDF.select(col("Title"),((col("IMDB_Rating") + (col("Rotten_Tomatoes_Rating")/10))/2) as("Overal_Rating"))
    .show()

  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating","IMDB_Rating"))

  //Strings

  val carsDF = spark.read
    .option("inferSchema","true")
    .json(data + "cars.json")

  carsDF.show()

  //Capitalize name initCap,lower,upper
  carsDF.select(initcap(col("Name")))
    .show()

  //contains
  carsDF.select("*").where(col("Name").contains("Chevrolet"))
    .show()

  val regex = "volkswagen|vw"
  carsDF.select(
    col("Name"),
    regexp_extract( col("Name"),regex,0).as("RegexExtracts")
  ).where(col("RegexExtracts") =!= "")
    .drop("RegexExtracts`")
  .show()


  carsDF.select(
    col("Name"),
    regexp_replace(col("Name"),regex,"Plus_sign_car").as("RegexReplace")
  )
    .show()

  def getCarsNames = List("VolksWagen","Mercedes-Benz","Ford")

  val complexRegex = getCarsNames.map(_.toLowerCase()).mkString("|")


  carsDF.select(
    col("Name"),
    regexp_extract( col("Name"),complexRegex,0).as("RegexExtracts")
  ).where(col("RegexExtracts") =!= "")
    .drop("RegexExtracts`")
    .show()


  val carNameFilterList = getCarsNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFiler = carNameFilterList.fold(lit(false))((combinedFilter,newCarFilter) =>combinedFilter or newCarFilter)

  carsDF.filter(bigFiler)
    .show()

}

