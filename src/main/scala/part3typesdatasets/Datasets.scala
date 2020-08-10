package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, col, element_at}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}


object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dateset of the complex type
  // 1 - define case class
  case class Car(
                  name: String,
                  miles_per_Gallon: Option[Double],
                  cylinders: Long,
                  displacement: Double,
                  horsepower: Long,
                  weight_in_lbs: Long,
                  acceleration: Double,
                  year: String,
                  origin: String
                )

  // 2 - read file from the file
  def readDF(filename: String) = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")
  // 3 - define Encoder

  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  case class CarWithNameAndHorsepower(Name: String, Horsepower: Long)
  carsDS.map(car => CarWithNameAndHorsepower(car.name, car.horsepower))

  case class Guitar(id:Long, model:String, make:String, guitarType: String)
  val guitarsDS = readDF("guitars.json").as[Guitar]

  case class GuitarPlayer(id:Long, name:String, guitars:Seq[Long], band:Long)
  case class Band(id:Long, name: String, hometown:String, year:Long)

  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandDS = readDF("bands.json").as[Band]

  val guitarPlayersWithBands : Dataset[(GuitarPlayer, Band)] = guitarPlayerDS
    .joinWith(bandDS, guitarPlayerDS.col("band") === bandDS.col("id"), "left")


  /**
    * Exercise
    */

  guitarPlayerDS.joinWith(guitarsDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "left")
  .select(element_at(col("_1").getField("guitars"), 1))
}
