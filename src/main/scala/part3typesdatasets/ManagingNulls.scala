package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"))
  )

  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNotNull)

  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  moviesDF.select("Title", "IMDB_Rating").na.drop()
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10
  ))

  moviesDF.selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(IMDB_Rating, 10)",
    "nvl(IMDB_Rating, 10)",
    "nullif(IMDB_Rating, 10)",
  )
}
