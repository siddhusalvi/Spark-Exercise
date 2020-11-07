import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object PostgresOperations extends App {
  val spark = SparkSession.builder()
    .config("spark.master","local[*]")
    .appName("PostgressOps")
    .getOrCreate()

  val someData = Seq(
    Row(8, "bat"),
    Row(64, "mouse"),
    Row(-27, "horse")
  )

  val someSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )

  val url = "jdbc:postgresql://localhost:5432/sparkfiles"
  val connectionProperties = new Properties()
  connectionProperties.put("Driver", "org.postgresql.Driver")
  connectionProperties.put("user", "postgres")
  connectionProperties.put("password", "admin")
  val query1 = "(SELECT * FROM firsttable) as q1"

  val dataframe = spark.read.jdbc(url, "firsttable", connectionProperties)

  dataframe.show()

//  val url = "jdbc:postgresql://localhost:5432"
//  val properties = new Properties()
//  properties.put("user", "postgres")
//  properties.put("password", "admin")
//  Class.forName("com.mysql.jdbc.Driver")
//  val dbTable = "sparkfiles." + "table1"
//  someDF.write.mode(SaveMode.Overwrite).jdbc(url, dbTable, properties)
}
