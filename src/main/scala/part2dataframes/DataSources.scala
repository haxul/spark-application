package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()


  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

//  carsDF.write.format("json")
//    .mode(SaveMode.Append)
//    .option("path", "src/main/resources/data/cars_result.json")
//    .save()

    val anotherCars = spark.read
      .format("json")
      .schema(carsSchema)
      .option("dateFormat", "YYYY-MM-dd")
      .option("mode", "failFast")
      .option("allowSingleQuotes", "true")
      .option("compression", "uncompressed")
      .json("src/main/resources/data/cars.json")

  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", StringType),
    StructField("price", DoubleType),
  ))

  val stocksDF  = spark.read
    .schema(stockSchema)
    .format("csv")
//    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

//  carsDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/cars.parquet")
  val path = "src/main/resources/data"
  spark.read.text(s"$path/sampleTextFile.txt")

  spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()


  /**
    * Exercise
    */

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .json(s"$path/movies.json")

  moviesDF.write
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .format("csv")
    .option("path", "src/main/resources/data/movies.csv")
    .save()

  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.test_table")
    .mode(SaveMode.Overwrite)
    .save()
}
