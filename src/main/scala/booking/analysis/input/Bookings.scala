package booking.analysis.input

import booking.analysis.domain.Booking
import booking.analysis.domain.Booking.isEligibleForAnalysis
import org.apache.spark.sql.{Dataset, SparkSession}

object Bookings {

  case class AnalysisData(noOfPassengers: Int,
                          adults: Int,
                          children: Int,
                          totalWeight: Int,
                          ageSum: Int,
                          noOfPassengersWithAgeInfo: Int)

  case class AnalysisKey(country: String, season: String, weekday: String)

  def load(spark: SparkSession, path: String): Dataset[Booking] = {
    import spark.implicits._
    spark.read.textFile(path)
      .map(Booking.fromJson(_))
      .filter(_.isDefined).map(_.get)
  }

  def eligibleForAnalysis(bookings: Dataset[Booking],
                          startUtc: String,
                          endUtc: String,
                          airportsToCountry: Map[String, String]) = {
    bookings
      .filter(b => isEligibleForAnalysis(b,
        startUtc,
        endUtc,
        airportsToCountry))
  }
}
