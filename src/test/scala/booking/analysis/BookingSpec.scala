package booking.analysis

import booking.analysis.Booking.{Flight, containsKlmFlight, isConfirmed, isOriginatingFromNetherlands}
import org.scalatest.flatspec._
import org.scalatest.matchers.should._

class BookingSpec extends AnyFlatSpec with Matchers  {

  val confirmedKlmFlight = Booking("2019-03-17T13:47:40.317Z", Seq.empty, Seq(
    Flight("CONFIRMED", "KL", "AMS", "DXB", "2019-03-17T17:10:00Z", "2019-03-18T02:55:00Z"),
    Flight("CONFIRMED", "DL", "DXB", "AMS", "2019-03-29T01:55:00Z", "2019-03-29T06:20:00Z")))

  val unconfirmedNonKlmFlight = Booking("2019-03-17T13:47:25.974Z", Seq.empty, Seq(
    Flight("CONFIRMED", "AF", "CDG", "BUD", "2019-03-14T20:55:00Z", "2019-03-14T23:05:00Z"),
    Flight("CANCELLED", "AF", "BUD", "CDG", "2019-03-17T16:25:00Z", "2019-03-17T18:45:00Z")))

  "Booking json line" should "be parsed correctly" in {
    val jsonWithAgeInformation = os.read(os.pwd/"src"/"test"/"resources"/ "event-with-age.json")
    val booking = Booking.fromJson(jsonWithAgeInformation).get
    val passengers = booking.passengers
    val flights = booking.flights
    booking.timestamp shouldBe "2019-03-17T13:47:26.005Z"
    passengers.size shouldBe 1
    passengers(0).age shouldBe Some(18)
    passengers(0).uci shouldBe "20062C080003A785"
    passengers(0).category shouldBe "ADT"
    passengers(0).weight shouldBe 22
    flights.size shouldBe 4
    val lastFlight = flights(3)
    lastFlight.airline shouldBe "AF"
    lastFlight.origin shouldBe "ORY"
    lastFlight.destination shouldBe "TLS"
    lastFlight.departureDate shouldBe "2019-07-17T20:50:00Z"
    lastFlight.arrivalDate shouldBe "2019-07-17T22:05:00Z"
    lastFlight.status shouldBe "CONFIRMED"
  }

  "Booking json line" should "be parsed correctly even there is no age information" in {
    val jsonWithoutAgeInformation = os.read(os.pwd/"src"/"test"/"resources"/"event-without-age.json")
    val booking = Booking.fromJson(jsonWithoutAgeInformation).get
    booking.passengers.size shouldBe 8
    booking.passengers(0).age shouldBe None
  }

  "Booking json with missing mandatory lines" should "be processed but should yield empty Booking" in {
    val invalidBooking = os.read(os.pwd/"src"/"test"/"resources"/"event-with-missing-fields.json")
    Booking.fromJson(invalidBooking) shouldBe None
  }

  "Contains any KLM flight" should "yield correct result" in {
    containsKlmFlight(confirmedKlmFlight) shouldBe true
    containsKlmFlight(unconfirmedNonKlmFlight) shouldBe false
  }

  "Is confirmed" should "yield correct result" in {
    isConfirmed(confirmedKlmFlight) shouldBe true
    isConfirmed(unconfirmedNonKlmFlight) shouldBe false
  }

  "Is originating from the Netherlands" should "yield correct result" in {
    isOriginatingFromNetherlands(confirmedKlmFlight, Map("AMS" -> "Netherlands")) shouldBe true
    isOriginatingFromNetherlands(unconfirmedNonKlmFlight, Map("AMS" -> "Netherlands", "CDG" -> "FRANCE")) shouldBe false
  }

}
