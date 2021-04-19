package booking

import java.io.{File, FileOutputStream}
import java.util.UUID

import booking.analysis.{Airports, Bookings}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analysis {

  val logger = Logger.getLogger(Analysis.getClass)

  def main(args: Array[String]): Unit = {
//    val Array(start, end, bookingsPath) = args
//    val startUtc: String = LocalDate.parse(start).atStartOfDay(ZoneId.of("UTC")).toInstant().toString
//    val endUtc: String = LocalDate.parse(end).atTime(23, 59, 59).atZone(ZoneId.of("UTC")).toString
//    val airports = pathOfAirportsDataFile()
    val spark = SparkSession.builder
      .appName("booking-analysis")
      .config(new SparkConf())
      .master("local[4]") //TODO: Remove before finalising!
      .getOrCreate()
    logger.info("Starting Analysis")
//    val logData = spark.read.textFile(airports).cache()
//    println(s"Rows in airports: ${logData.count()}")

    val base = s"${System.getProperty("user.home")}/dev/assignment-booking-analysis/data"
    val bookingsFile = s"$base/bookings/booking.json"
    val airports = s"$base/airports/airports.dat"
    val logData = Airports.toLookup(spark, airports)
    val airportsToCountry = logData.map(m => (m._1, m._2._1))
    val broadCastAirports = spark.sparkContext.broadcast(airportsToCountry)
    val startUtc = "2010-02-05T13:05:00Z"
    val endUtc = "2025-02-05T13:05:00Z"

    val bookings = Bookings.load(spark, bookingsFile)
    val eligibleBookings = Bookings.eligibleForAnalysis(bookings, startUtc, endUtc, broadCastAirports.value)
    val flattenedFlights = Bookings.flattenFlights(spark, eligibleBookings, broadCastAirports.value)
    val analysisSet = Bookings.toAnalysisDataSet(spark, flattenedFlights, broadCastAirports.value)
    import spark.implicits._
    val finalResult = Bookings.aggregateToFinalReport(spark, analysisSet)
      .rdd
      .sortBy(_._2.noOfPassengers, false )
      .toDS()
    finalResult.show(finalResult.count().toInt, false)
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
