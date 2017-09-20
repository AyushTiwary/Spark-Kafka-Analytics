package knoldus

import java.sql.Timestamp
import knoldus.GlobalObject.{mySchema, spark}
import org.apache.spark.sql.functions._

object PubSub extends App {

  val csvDF1 = spark
    .readStream
    .schema(mySchema)
    .csv("src/main/resources/")

  /*
  * Function to send the data to Kafka from streaming DataFrame
  * */
  csvDF1.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
    writeStream
    .format("kafka")
    .option("topic", "movieData")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/knoldus/Desktop/POCnD")
    .start()

  /*
   * Function to rend the data from Kafka to streaming DataFrame
   * */
  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "movieData")
    .load()

  val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
    .select(from_json($"value", mySchema).as("data"), $"timestamp")
    .select("data.*", "timestamp")

    val result1 = df1.groupBy().agg(sum("duration").as("sum"), min("duration").as("min"), max("duration").as("max"),
    avg("duration").as("average"))

/*  result1.writeStream
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()*/

/*  df1.writeStream
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()*/

}
