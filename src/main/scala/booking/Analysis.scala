package booking

import java.io.{File, FileOutputStream}
import java.util.UUID

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analysis {

  val logger = Logger.getLogger(Analysis.getClass)

  def main(args: Array[String]): Unit = {
    val airports = pathOfAirportsDataFile()
    val spark = SparkSession.builder
      .appName("booking-analysis")
      .config(new SparkConf())
      .master("local[4]") //TODO: Remove before finalising!
      .getOrCreate()
    logger.info("Starting Analysis")
    val logData = spark.read.textFile(airports).cache()
    println(s"Rows in airports: ${logData.count()}")
    spark.stop()
  }

  private def pathOfAirportsDataFile(): String = {
    import resource._
    val randomFileName = UUID.randomUUID().toString + ".dat"
    val tempFolder = System.getProperty("java.io.tmpdir")
    val logFile = new File(tempFolder, randomFileName)
    val copyFromJar = for {input <- managed(getClass.getResourceAsStream("/airports.dat"))
         output <- managed(new FileOutputStream(logFile))
         } yield {
      input.transferTo(output)
    }
    val result : Either[List[Throwable], Long]= copyFromJar.acquireFor(identity)
    result match {
      case Left(exceptions) => {
        val dueTo = exceptions.head
        logger.error(s"Error while reading airports data. Due to: $dueTo")
        throw new RuntimeException()
      }
      case Right(_) => logFile.getPath
    }
  }


}
