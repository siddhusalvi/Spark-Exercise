import java.sql.Date

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Datasets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Dataset")
      .master("local")
      .getOrCreate()

    val path = "src/main/resources/data/"

    val numbersDf = spark.read
      .format("csv")
      .option("header","true")
      .load(path + "numbers.csv")

//    numbersDf.show()
    implicit val encoders =Encoders.scalaInt

    val numbersDS:Dataset[Int]=numbersDf.as[Int]

    //converts dataset into dataframe
    numbersDS.filter(_<100)
      .show()

    //dataset of complex type

    case class  Car(
                     Name:String,
                     Miles_per_Gallon:Int,
                     Cylinders:Int,
                     Displacement:Double,
                     Horsepower:Int,
                     Weight_in_lbs:Int,
                     Acceleration:Double,
                     Year:Date,
                     Origin:String
                   )

  }
}
