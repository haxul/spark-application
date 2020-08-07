package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col}

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

  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), array_contains(col("guitars"), col("guitarId")), "left")



  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .load()

  val managerDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.dept_manager")
    .load()

  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.titles")
    .load()
  // 1
  val condition  = employeesDF.col("emp_no") === salariesDF.col("emp_no")
  employeesDF.join(salariesDF, condition , "left")
    .select(employeesDF.col("emp_no"), salariesDF.col("salary")).groupBy("emp_no").max("salary")
  // 2

  employeesDF.join(managerDF, employeesDF.col("emp_no") === managerDF.col("emp_no"), "left_anti")

  // 3

  employeesDF.join(salariesDF, employeesDF.col("emp_no") === salariesDF.col("emp_no"), "left")
    .select(employeesDF.col("emp_no"), salariesDF.col("salary"))
    .groupBy("emp_no")
    .max("salary")
    .join(titlesDF, employeesDF.col("emp_no") === titlesDF.col("emp_no"), "left")
    .select(employeesDF.col("emp_no"), col("max(salary)"), col("title")).
    orderBy(col("max(salary)").desc)



}

