//UDFExamples
import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, from_json, _}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.{DataTypes, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import org.apache.spark.sql.functions.udf


object UDFExamples {
  def main(args: Array[String]): Unit = {
//    try {
      val spark = SparkSession.builder()
        .appName("Exercise")
        .master("local[*]")
        .getOrCreate()

//      val firstDF = spark.read
//        .format("json")
//        .option("inferSchema","true")
//        .load("src/main/resources/data/cars.json")


//      firstDF.show()

//      firstDF.printSchema()

//      val data = firstDF.take(10).foreach(println(_))

//      val data = firstDF.take(10).flatMap(_.toSeq)
//        println(data.mkString(" "))

//      import spark.implicits._
//      val DFtoSeq = firstDF.take(10).map(_.toSeq)
//      DFtoSeq.foreach(println(_))

//      import spark.implicits._
//      val DFtoArray = firstDF.take(10)
//        .map(_.toSeq)
//        .map(_.toArray)
//
//      DFtoArray.foreach{
//        record => val arr = record.toArray
//          println(arr.mkString(" "))
//      }

//      val flattenArray = DFtoArray.flatMap(record => record.toSeq)
//      flattenArray.foreach(println(_))

//            val flattenArray = DFtoArray
//              .flatMap(record => record.toSeq)
//                .toArray
//      println(flattenArray.mkString(" "))


      val tablename = "work"
      val sc = spark.sparkContext
      val logDfSchema = getDfSchema()
      val dir = "src\\resources\\"
      val path = dir + "userWorkData.csv"
      val prePath = "src/main/resources/day ("
      val postPath = ").csv"

      val userData = getDfFromCsv(prePath + "2" + postPath, spark, logDfSchema)

//      userData.show()

      val impData = userData.select("User_Name","DateTime","Keyboard","Mouse")
        .withColumn("Action",(col("Keyboard") + col("Mouse") ).cast("Integer"))
          .drop("Keyboard")
          .drop("Mouse")
          .withColumnRenamed("User_Name","ID")
        .orderBy("ID","DateTime")
          .drop("DateTime")


          import spark.implicits._
        val len = impData.count().toInt
          val DFtoArray = impData.take(len)
            .map(_.toSeq)
            .map(_.toArray)

                val flattenArray = DFtoArray
                  .flatMap(record => record.toSeq)
                    .toArray
          println(flattenArray.mkString(" "))

//        def calculateIdleTime =





//    } catch {
//      case exception => println(exception)
//      case exception1:ClassNotFoundException => println(exception1)
//      case exception2:sql.AnalysisException => println(exception2)
//    }
  }

  //Function to get all data
  def getAllData(spark: SparkSession): DataFrame = {
    val logDfSchema = getDfSchema()
    val prePath = "src/resources/day ("
    val postPath = ").csv"

    val logDF1 = getDfFromCsv(prePath + "1" + postPath, spark, logDfSchema)
    val logDF2 = getDfFromCsv(prePath + "2" + postPath, spark, logDfSchema)
    val logDF3 = getDfFromCsv(prePath + "3" + postPath, spark, logDfSchema)
    val logDF4 = getDfFromCsv(prePath + "4" + postPath, spark, logDfSchema)
    val logDF5 = getDfFromCsv(prePath + "5" + postPath, spark, logDfSchema)
    val logDF6 = getDfFromCsv(prePath + "6" + postPath, spark, logDfSchema)
    val logDF7 = getDfFromCsv(prePath + "7" + postPath, spark, logDfSchema)
    val logDF8 = getDfFromCsv(prePath + "8" + postPath, spark, logDfSchema)
    val logDF9 = getDfFromCsv(prePath + "9" + postPath, spark, logDfSchema)

    val logDF = logDF1.union(logDF2).union(logDF3).union(logDF4).union(logDF5).union(logDF6).union(logDF7).union(logDF8).union(logDF9)
    logDF
  }

  //Function to get csv dataframe
  def getDfFromCsv(path: String, spark: SparkSession, strct: StructType): DataFrame = {
    val DF = spark.read
      .format("csv")
      .option("header", true)
      .option("timestampFormat", "MM/dd/yyyy HH:mm:ss.SSSSSS")
      .schema(strct)
      .load(path)
    DF
  }

  //Function to get dataframe schema
  def getDfSchema(): StructType = {
    val schema = StructType(
      Array(
        StructField("DateTime", TimestampType), //===================
        StructField("Cpu_Count", IntegerType),
        StructField("Cpu_Working_Time", DoubleType),
        StructField("Cpu_Idle_Time", DoubleType),
        StructField("Cpu_Percent", DoubleType),
        StructField("Usage_Cpu_Count", IntegerType),
        StructField("Software_Interrupts", IntegerType),
        StructField("System_calls", IntegerType),
        StructField("Interrupts", IntegerType),
        StructField("Load_Time_1_min", DoubleType),
        StructField("Load_Time_5_min", DoubleType),
        StructField("Load_Time_15_min", DoubleType),
        StructField("Total_Memory", DoubleType),
        StructField("Used_Memory", DoubleType),
        StructField("Free_Memory", DoubleType),
        StructField("Active_Memory", DoubleType),
        StructField("Inactive_Memory", DoubleType),
        StructField("Bufferd_Memory", DoubleType),
        StructField("Cache_Memory", DoubleType),
        StructField("Shared_Memory", DoubleType),
        StructField("Available_Memory", DoubleType),
        StructField("Total_Disk_Memory", DoubleType),
        StructField("Used_Disk_Memory", DoubleType),
        StructField("Free_Disk_Memory", DoubleType),
        StructField("Read_Disk_Count", IntegerType),
        StructField("Write_Disk_Count", IntegerType),
        StructField("Read_Disk_Bytes", DoubleType),
        StructField("Write_Disk_Bytes", DoubleType),
        StructField("Read_Time", IntegerType),
        StructField("Write_Time", IntegerType),
        StructField("I/O_Time", IntegerType),
        StructField("Bytes_Sent", DoubleType),
        StructField("Bytes_Received", DoubleType),
        StructField("Packets_Sent", IntegerType),
        StructField("Packets_Received", IntegerType),
        StructField("Errors_While_Sending", IntegerType),
        StructField("Errors_While_Receiving", IntegerType),
        StructField("Incoming_Packets_Dropped", IntegerType),
        StructField("Outgoing_Packets_Dropped", IntegerType),
        StructField("Boot_Time", StringType),
        StructField("User_Name", StringType),
        StructField("Keyboard", DoubleType),
        StructField("Mouse", DoubleType),
        StructField("Technologies", StringType),
        StructField("Files_Changed", IntegerType)
      )
    )
    schema
  }
}
