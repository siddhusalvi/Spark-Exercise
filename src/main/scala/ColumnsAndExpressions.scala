import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DFColumsAndExpression")
    .master("local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  val firstColumn = carsDF.col("Name")


  val carsNameDf = carsDF.select(firstColumn)

  carsNameDf.show()

  import  spark.implicits._
  carsDF.select(
  carsDF.col("Name"),
    col("Acceleration"),
    column("weight_in_lbs"),
    'Year,
    $"Horsepower",
    expr("origin")
   ).show()

  carsDF.select("Name","Year").show()

  //EXPRESSIONS

  val simplestExpressions = carsDF.col("Weight_in_lbs")

  val weightInKGExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeight = carsDF.select(
    col ( "Name"),
    col (  "Weight_in_lbs"),
    weightInKGExpression,
    expr("Weight_in_lbs / 2.2").as("wightINKG")
  )

  carsWithWeight.show()


  carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2").show()

  //Adding a column
    carsDF.withColumn("Weight_in_Kg",col("Weight_in_lbs") / 2.2).show( )

  //Renaming a column

  carsDF.withColumnRenamed("Name","name").show()

  carsDF.drop("Name","Weight_in_lbs").show()

  carsDF.filter(col("Origin") =!= "USA").show()

  carsDF.where(col("Origin") =!= "USA").show()

  carsDF.filter("Origin = 'USA'").show()

  //chain fitler

  carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150).show()

  carsDF.filter(col("Origin") === "USA" and(col("Horsepower") > 150)).show()

//  adding more rows

val moreCarsDF = spark.read
  .option("inferSchema","true")
  .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF)

  allCarsDF.show()

  val allCountriesDF = carsDF.select("Origin").distinct()

  allCountriesDF.show()
}

