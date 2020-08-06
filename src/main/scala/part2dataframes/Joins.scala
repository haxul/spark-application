package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, expr, max}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val path = "src/main/resources/data"

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$path/guitars.json")

  val guitaristsDF = spark.read
    .option("inferScheme", "true")
    .json(s"$path/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferScheme", "true")
    .json(s"$path/bands.json")

  // left_semi, left_anti
  val joinCondition =  guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition , "left")

  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  guitaristsBandsDF.drop(bandsDF.col("id"))

  val modBandsIdDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(modBandsIdDF, guitaristsDF.col("band") === modBandsIdDF.col("bandId"), "left")

  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), array_contains(col("guitars"), col("guitarId")), "left").show()


}

