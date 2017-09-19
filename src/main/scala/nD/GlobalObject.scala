package nD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object GlobalObject {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("Spark-Batch-Streaming-Analytics")
    .master("local")
    .getOrCreate()

  val filePath: String = "src/main/resources/moviesdataset.txt"

  val mySchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("year", IntegerType),
    StructField("rating", DoubleType),
    StructField("duration", IntegerType)
  ))

}
