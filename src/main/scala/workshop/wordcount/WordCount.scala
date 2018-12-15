package workshop.wordcount

import java.time.Clock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application.conf")
    log.info("Config file used: " + conf.origin)

    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.master("local[2]").appName("Spark Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val itemInfoPath = conf.getString("apps.WordCount.item")
    val orderInfoPath = conf.getString("apps.WordCount.order")

    val outputPath = conf.getString("apps.WordCount.output")

    run(spark, itemInfoPath, orderInfoPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, itemInfoPath: String, orderInfoPath: String, outputPath: String): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._
    println("INFO PATH:" + itemInfoPath + " ORDER PATH " + orderInfoPath + " OUTPUT PATH " + outputPath)

    val itemInfo = spark.read
      .option("inferSchema", "true")
      .option("header", "true") // Option telling Spark that the file has a header
      .csv(itemInfoPath)
    val orderInfo = spark.read.option("inferSchema", "true").csv(orderInfoPath).toDF("MONTH", "ORDER_ID", "SKU_ID", "AMOUNT")
    val fullInfo = orderInfo
      .join(itemInfo, "SKU_ID")

    val perProductPerMonthDF = fullInfo
      .groupBy("NAME", "MONTH")
      .agg(sum("AMOUNT") as "total")
      .withColumn("SIZE", lit("ALL"))
      .orderBy("NAME", "MONTH")

    val allProductPerMonthDF = fullInfo
      .groupBy("MONTH")
      .agg(sum("AMOUNT") as "total")
      .withColumn("SIZE", lit("ALL"))
      .withColumn("NAME", lit("ALL"))

    val allMonthPerProductDF = fullInfo
      .groupBy("NAME")
      .agg(sum("AMOUNT") as "total")
      .withColumn("SIZE", lit("ALL"))
      .withColumn("MONTH", lit("ALL"))

    val allSizePerMonthDF = fullInfo
      .groupBy("SIZE")
      .agg(sum("AMOUNT") as "total")
      .withColumn("MONTH", lit("ALL"))
      .withColumn("NAME", lit("ALL"))

    val resultDF = allMonthPerProductDF
      .unionByName(allProductPerMonthDF)
      .unionByName(allSizePerMonthDF)
      .orderBy("NAME", "MONTH","SIZE")

    resultDF.write.csv(outputPath)
  }
}
