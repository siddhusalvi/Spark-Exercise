package util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object covidCleaning {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CovidCleaning")
      .master("local")
      .getOrCreate()

    val schema = StructType(
      Array(
        StructField("Date", StringType, true),
        StructField("State", StringType, true),
        StructField("TotalSamples", DoubleType, true),
        StructField("Negative", DoubleType, true),
        StructField("Positive", DoubleType, true),
      )
    )

    val covidDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("dateFormat", "yyyy/MM/dd")
      .schema(schema)
      .load("src/main/resources/data/swt.csv")


    covidDF.na.drop()
      .orderBy(col("Date").desc)
      .dropDuplicates("State")
      .show(300)
  }
}
  /*

    val schema = StructType(
      Array(
          StructField("agebracket", StringType, true),
          StructField("backupnotes", StringType, true),
          StructField("contractedfromwhichpatientsuspected", StringType, true),
          StructField("currentstatus", StringType, true),
          StructField("dateannounced", DateType, true),
          StructField("detectedcity", StringType, true),
          StructField("detecteddistrict", StringType, true),
          StructField("detectedstate", StringType, true),
          StructField("estimatedonsetdate", StringType, true),
          StructField("gender", StringType, true),
          StructField("nationality", StringType, true),
          StructField("notes", StringType, true),
          StructField("numcases", StringType, true),
          StructField("patientnumber", StringType, true),
          StructField("source1", StringType, true),
          StructField("source2", StringType, true),
          StructField("source3", StringType, true),
          StructField("statecode", StringType, true),
          StructField("statepatientnumber", StringType, true),
          StructField("statuschangedate", DateType, true),
          StructField("typeoftransmission", StringType, true)
      )
    )
\ion("dateFormat", "dd/MM/yyyy")
      .json("src/main/resources/data/covidData.json")
    covidDF.show(300)
  }


  }
   */
