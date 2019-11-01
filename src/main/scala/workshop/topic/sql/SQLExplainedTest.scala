package workshop.topic.sql

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import workshop.schema.TaxiRide

object SQLExplainedTest {
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application.conf")
    log.info("Config file used: " + conf.origin)

    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.master("local[2]").appName("Spark Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val nycTaxiDataPath = getClass.getResource(conf.getString("apps.source_data.nyc_taxi")).getFile

    val nycTaxiData = spark.read.textFile(nycTaxiDataPath)

    import spark.implicits._

    val taxiRideDS: Dataset[TaxiRide] = nycTaxiData.map(line => TaxiRide.fromString(line)).as[TaxiRide]

    println(taxiRideDS.explain())

    spark.stop()
  }
}
