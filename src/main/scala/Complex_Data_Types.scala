import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Complex_Data_Types extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("ComplexDataTypes")
    .getOrCreate()

  val path = "src/main/resources/data/"

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json(path + "movies.json")

  moviesDF.show()

  moviesDF.select(col("Title"),to_date(col("Release_Date"),"dd-MMM-YY")as("Release"))
    .show()

  //Adding current date and timestamp
  moviesDF.select(current_date().as("Today"),current_timestamp().as("Entry"))
    .show()

  //calculating date difference it returns days
  //if any problem occurs during dates spark will return null
  moviesDF.select("Title","Release_Date")
    .withColumn("Release",to_date(col("Release_Date"),"dd-MMM-YY"))
    .withColumn("Current_Date",current_date())
    .withColumn("Age",(datediff(col("Current_Date"),col("Release"))/365))
    .show()

//when date is in multiple format we parse datframe using each possible format and then join them

  val StocksDF = spark.read
    .option("inferSchema", "true")
    .option("header","true")
    .csv(path + "stocks.csv")

//  StocksDF.show()

  StocksDF.withColumn("Dates",to_date(col("date"),"MMM dd yyyy"))
    .show()

  //Structures
  moviesDF.select(col("Title"),struct(col("Us_Gross"),col("Worldwide_Gross")).as("Total_Revenue"))
    .show()

  //Extracting values from tuple
  moviesDF.select(col("Title"),struct(col("Us_Gross"),col("Worldwide_Gross")).as("Total_Revenue"))
  .select(col("Title"),col("Total_Revenue"),col("Total_Revenue").getField("Us_Gross").as("US_Profit"))
//     .show()

  //with expression strings
  moviesDF.selectExpr("Title","(US_Gross,WorldWide_Gross) as Profit")
    .selectExpr("Title","Profit.US_Gross")
    .show()

  //Arrays
  val moviesWordsDF = moviesDF.select(col("Title"),split(col("Title")," |,").as("Split_Data"))

  //common array operations
  moviesWordsDF.select(col("Title"),
    expr("Split_Data[0]"),
    size(col("Split_Data")),
    array_contains(col("Split_Data"),"love")
  )
    .show()

}
