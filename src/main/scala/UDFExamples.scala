//UDFExamples

import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

case class WorkData(ID: String, Idle: Int)

case class UserTime(ID: String, time: Int)

object UDFExamples {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder()
        .appName("Exercise")
        .master("local[*]")
        .getOrCreate()

      val tablename = "work"
      val sc = spark.sparkContext
      val logDfSchema = getDfSchema()
      val dir = "src\\resources\\"
      val path = dir + "userWorkData.csv"
      val prePath = "src/main/resources/day ("
      val postPath = ").csv"

      val userData = getDfFromCsv(prePath + "2" + postPath, spark, logDfSchema)
      //userData.show()

      val impData = userData.select("User_Name", "DateTime", "Keyboard", "Mouse")
        .withColumn("Action", (col("Keyboard") + col("Mouse")).cast("Integer"))
        .drop("Keyboard")
        .drop("Mouse")
        .withColumnRenamed("User_Name", "ID")
        .orderBy("ID", "DateTime")
        .drop("DateTime")


      def getColumnToArray(DF: DataFrame, column: String): Array[Any] = {
        DF.select(column).take(DF.count().toInt)
          .map(_.toSeq)
          .map(_.toArray)
          .flatMap(record => record.toSeq)
          .toArray
      }


      val userIds = getColumnToArray(impData, "ID").map(_.toString);
      val userStauts = getColumnToArray(impData, "Action").map(record => record.toString.toInt)

      def getWorkData(ids: Array[String], staus: Array[Int]): Any /*Array[WorkData]*/ = {
        var workrecord = new ListBuffer[WorkData]();
        var totalWorkTime = new ListBuffer[UserTime]();
        var ID = ""
        var counter = 0
        var userWorkCounter = 0


        def writeRecord(index: Int): Unit = {
          if (counter > 5) {
            workrecord += WorkData(ids(index), counter)
          }
        }

        def writeTotalWork(index: Int): Unit = {
          totalWorkTime += UserTime(ids(index), userWorkCounter * 5)
        }


        for (i <- 0 until (staus.length)) {
          //First record
          if (i == 0) {
            ID = ids(i)
            if (staus(i) == 0) {
              counter = 1
            }
            //Last record
          } else if (i + 1 == staus.length - 1) {
            if (!ID.equals(ids(i))) {
              writeRecord(i - 1)
              counter = 0
              ID = ids(i)
              writeTotalWork(i - 1)
              userWorkCounter = 1
              if (staus(i) == 0) {
                counter = 1
              }
              writeRecord(i)
              writeTotalWork(i)
            } else {
              if (staus(i) == 0) {
                counter += 1
              }
              writeRecord(i)
              userWorkCounter += 1
              writeTotalWork(i)
            }
          }

          else {
            if (!ID.equals(ids(i))) {
              writeRecord(i - 1)
              writeTotalWork(i - 1)
              userWorkCounter = 0
              ID = ids(i)
              if (staus(i) == 0) {
                counter = 1
              } else {
                counter = 0
              }
            } else {
              if (staus(i) != 0) {
                writeRecord(i)
                counter = 0
              } else {
                counter += 1
              }
            }
          }
          userWorkCounter += 1
        }

        workrecord.foreach(println(_))


      }

      val carSchema = StructType(
        Array(
          StructField("Name", StringType),
          StructField("Miles_per_Gallon", DoubleType),
          StructField("Cylinders", DoubleType),
          StructField("Displacement", DoubleType),
          StructField("Horsepower", DoubleType),
          StructField("Weight_in_lbs", DoubleType),
          StructField("Acceleration", DoubleType),
          StructField("Year", DateType),
          StructField("Origin", StringType),
        )
      )

      val jsonDF = spark.read
        .format("json")
        .option("DateFomat", "YYYY-MM-dd")
        .schema(carSchema)
        .option("compression", "uncompressed")
        .option("allowSingleQuetoes", "true")
        .load("src/main/resources/data/cars.json")

      //    jsonDF.show()

      val stockSchema = StructType(
        Array(
          StructField("Symbol", StringType),
          StructField("Date", DateType),
          StructField("price", DoubleType)
        )
      )

      val stockCSV = spark.read
        .format("csv")
        .schema(stockSchema)
        .option("DateFomat", "MM-dd-YYYY")
        .option("header", "true")
        .option("sep", ",")
        .option("nullValue", "")
        .load("src/main/resource/data/stocks.csv")

      stockCSV.show()

    } catch {
      case exception => println(exception)
      case exception1: ClassNotFoundException => println(exception1)
      case exception2: sql.AnalysisException => println(exception2)
    }
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
