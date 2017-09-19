package nD

import nD.GlobalObject._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MovieBatchTask extends App {

  val csvDF: DataFrame = spark.read.schema(mySchema).csv(filePath)

  val result = csvDF.groupBy().agg(sum("duration").as("sum"), min("duration").as("min"), max("duration").as("max"),
    avg("duration").as("average"))

  result.show()

}
