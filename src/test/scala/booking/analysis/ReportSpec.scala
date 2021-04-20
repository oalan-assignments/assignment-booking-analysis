package booking.analysis

import booking.analysis.Report.{AnalysisData, AnalysisKey, ReportRow, analysisToReportRow, flightToAnalysisKey, passengersToAnalysisData}
import booking.analysis.domain.Booking.{Flight, Passenger}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReportSpec extends AnyFlatSpec with Matchers {

  "Flight to Analysis Key" should "work as expected" in {
    val departureOnMondayUtc = "2021-04-19T23:00:00Z"
    val flight = Flight("CONFIRMED", "KL", "AMS", "BUD",
      departureOnMondayUtc, "2021-04-19T03:30:00Z")
    val airportToCountry = Map("AMS" -> "Netherlands", "BUD" -> "Hungary")
    val airportToTimezone = Map("AMS" -> "Europe/Amsterdam")

    flightToAnalysisKey(flight, airportToCountry, airportToTimezone) shouldBe
      AnalysisKey("Hungary", "Spring", "TUESDAY")
  }

  "Passengers to Analysis Data" should "work as expected" in {
    val passengers = Seq(
      Passenger("1", "ADT", None, 70),
      Passenger("2", "CHD", Some(12), 40),
      Passenger("3", "CHD", Some(8), 40)
    )
    passengersToAnalysisData(passengers) shouldBe
      AnalysisData(noOfPassengers = 3, adults = 1, children = 2,
        totalWeight = 150, ageSum = 20, noOfPassengersWithAgeInfo = 2)
  }

  "Analysis data to Report Row" should "work as expected" in {
    val key = AnalysisKey("Hungary", "Spring", "TUESDAY")
    analysisToReportRow((key, AnalysisData(noOfPassengers = 3, adults = 1, children = 2,
      totalWeight = 150, ageSum = 20, noOfPassengersWithAgeInfo = 2))) shouldBe
      ReportRow(key.country,  key.season, key.weekday, 3, 1, 2, 50.0, Some(10.0))

    analysisToReportRow((key, AnalysisData(noOfPassengers = 3, adults = 1, children = 2,
      totalWeight = 150, ageSum = 0, noOfPassengersWithAgeInfo = 0))) shouldBe
      ReportRow(key.country,  key.season, key.weekday, 3, 1, 2, 50.0, None)
  }

}
