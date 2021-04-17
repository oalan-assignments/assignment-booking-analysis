package booking.analysis

import org.scalatest.flatspec._
import org.scalatest.matchers.should._

class BookingSpec extends AnyFlatSpec with Matchers  {

  "Booking json line" should "be parsed correctly" in {
    val jsonWithAgeInformation = os.read(os.pwd/"src"/"test"/"resources"/"single-event-with-age.json")
    val booking = Booking.fromJson(jsonWithAgeInformation).get
    booking.timestamp shouldBe "2019-03-17T13:47:26.005Z"
    booking.passengers.size shouldBe 1
    booking.passengers(0).age shouldBe Some(18)
    booking.passengers(0).uci shouldBe "20062C080003A785"
    booking.passengers(0).category shouldBe "ADT"
    booking.passengers(0).weight shouldBe 22
    booking.flights.size shouldBe 4
    booking.flights(3).airline shouldBe "AF"
    booking.flights(3).airline shouldBe "AF"
  }

  "Booking json line" should "be parsed correctly even there is no age information" in {
    val jsonWithoutAgeInformation = os.read(os.pwd/"src"/"test"/"resources"/"single-event-with-crid.json")
    val booking = Booking.fromJson(jsonWithoutAgeInformation).get
    booking.passengers.size shouldBe 8
    booking.passengers(0).age shouldBe None
  }
}
