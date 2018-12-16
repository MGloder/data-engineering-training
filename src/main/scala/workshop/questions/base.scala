package workshop.questions

import org.apache.spark.sql.SparkSession

class Base(sparkSession: SparkSession, productPath: String, orderPath: String) {
  val order = sparkSession
    .read
    .option("inferSchema", "true")
    .option("header", "false")
    .csv(orderPath)
    .toDF("MONTH", "ORDER_ID", "SKU_ID", "AMOUNT")

  val product = sparkSession
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(productPath)

}
