package workshop.questions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class answers(sparkSession: SparkSession, orderInfo: String, productInfo: String) extends Base(sparkSession, orderInfo, productInfo) {
  def q1(): DataFrame = order.join(product, "SKU_ID")

  def q3(): DataFrame = q1.groupBy("SKU_ID", "MONTH").count

  def q4(): DataFrame = q1.groupBy("SKU_ID").count.withColumn("MONTH", lit("ALL")).union(q3)

}
