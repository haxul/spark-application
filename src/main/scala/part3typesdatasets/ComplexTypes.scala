package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Release"))

  moviesWithReleaseDates.withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Release")) / 365)

  moviesWithReleaseDates.select("*").where(col("Release").isNull)


  val stocksDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.withColumn("to_date", to_date(col("date"), "MMM d yyyy"))


  moviesDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  ).select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
    .select(expr("Title_Words[0]"), col("Title"), size(col("Title_Words")), array_contains(col("Title_Words"), "Love")).show()
}
