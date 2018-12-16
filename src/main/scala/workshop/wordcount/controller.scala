package workshop.wordcount

import java.time.Clock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import workshop.questions.answers
import collection.JavaConversions._


object Controller {

  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application.conf")
    log.info("Config file used: " + conf.origin)

    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.master("local[2]").appName("Spark Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val productResource = getClass.getResource(conf.getString("apps.WordCount.product"))
    val orderResource = getClass.getResource(conf.getString("apps.WordCount.order"))

    val outputPath = conf.getString("apps.WordCount.output")

    val result = new answers(spark, productResource.getPath, orderResource.getPath)

    val a_list = conf.getStringList("apps.Questions").toList.map(result.getClass.getMethod(_).invoke(result))
//    a_list.map(spark.sparkContext.parallelize(_).option("header", "true").toDF().show)
    spark.stop()
  }
}




