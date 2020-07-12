import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins {
  def main(args :Array[String]){
  val spark = SparkSession.builder()
    .appName("joins")
    .master("local")
    .getOrCreate()

    val path = "src/main/resources/data/"

    val guitarsDF = spark.read
      .option("inferSchema","true")
      .json(path + "guitars.json")

//    guitarsDF.show()

    val guitaristsDF = spark.read
      .option("inferSchema","true")
      .json(path + "guitarPlayers.json")

//    guitaristsDF.show()

    val bandsDF = spark.read
      .option("inferSchema","true")
      .json(path + "bands.json")

//    bandsDF.show()

    //Joins

    val joinConditions = guitaristsDF.col("band") === bandsDF.col("id")

    val guitaristsBandDF = guitaristsDF.join(bandsDF,joinConditions,"inner")

    //left outer join : it will contain all data in left table with nulls
    guitaristsDF.join(bandsDF,joinConditions,"left_outer")
//      .show()

    //right outer join it will contain all data in right table

    guitaristsDF.join(bandsDF,joinConditions,"right_outer")
//      .show()

    //contains all data in both table
    guitaristsDF.join(bandsDF,joinConditions,"outer")
//      .show()

    //show only left table data in the join
    guitaristsDF.join(bandsDF,joinConditions,"left_semi")
//      .show()

//shows only left table data where no join relation
    guitaristsDF.join(bandsDF,joinConditions,"left_anti")
//      .show()

    //Rename the colums on which we are joining
    guitaristsDF.join(bandsDF.withColumnRenamed("id","band"),"band")
//        .show()

//    Delete either column in join connection
    guitaristsBandDF.drop(bandsDF.col("id"))
//      .show()

//rename the column
  val bandsModDf = bandsDF.withColumnRenamed("id","bandId")
//  bandsModDf.show()

    guitaristsDF.join(bandsModDf,guitaristsDF.col("band") === bandsDF.col("bandId"))
//      .show()

//using complex types joins using array

    guitaristsDF.join( guitarsDF.withColumnRenamed("id","guitarID"),expr("Array.contains(guitars,guitarID)"))
      .show()
  }


}
