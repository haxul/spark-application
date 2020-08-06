package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns")
    .config("spark.master", "local")
    .getOrCreate()

  val path = "src/main/resources/data"
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$path/cars.json")

  val firstColumn = carsDF.col("Name")
  val carNamesDF = carsDF.select(firstColumn)

  carsDF.select(
    col("Name"),
    col("Year"),
    expr("Origin")
  )

  val expressionsExample = carsDF.col("Name")
  val weightInKg = carsDF.col("Weight_in_lbs") / 2.2

  carsDF.select(
    col("Name"),
    weightInKg.as("Weight_in_kg").cast("Int")
  )

  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  carsDF.drop("Displacement")

  carsDF.filter(col("Origin") =!= "USA")
  carsDF.where(col("Origin") =!= "USA")

  carsDF.where(col("Origin") === "USA").where(col("Horsepower") > 150)
  carsDF.where(col("Origin") === "USA" and col("Horsepower") > 210)

  val moreCarsDF = spark.read.option("inferSchema", "true").json(s"$path/more_cars.json")
  carsDF.union(moreCarsDF).select("Origin").distinct()


  /**
  Exercises
   */


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$path/movies.json")

  moviesDF.select(col("Title"), col("Release_Date"))
  moviesDF.withColumn("total_income", col("US_Gross") + col("Worldwide_Gross") + when(col("US_DVD_Sales").isNull, 0).otherwise(col("US_DVD_Sales")))
  .select(
    col("Title"),
    col("total_income"),
    col("US_Gross"),
    col("Worldwide_Gross"), (when(col("US_DVD_Sales").isNull, 0).otherwise(col("US_DVD_Sales"))).as("US_DVD_Sales"))

  moviesDF.select(col("Title"), col("IMDB_Rating"), col("Major_Genre"))
    .where(col("IMDB_Rating") > 6 and col("Major_Genre").contains("Comedy"))

}