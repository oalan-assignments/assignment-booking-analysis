package booking.analysis.input

import booking.SparkSetup
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BookingsSpec extends AnyFlatSpec with Matchers with SparkSetup {

  "Given a data set with invalid record, bookings data" should
    "be loaded and records eligible for analysis filtered correctly" in withSparkSession { spark =>
    // this data set contains two klm ams flight events with identical flights, one non-klm flight and finally a booking
    // with weight information missing (invalid)
    val bookingsFile = s"${os.pwd.toString}/src/test/resources/bookings-sample.json"
    val bookings = Bookings.load(spark, bookingsFile)
    bookings.count() shouldBe 3
    bookings.take(1)(0).timestamp shouldBe "2019-03-17T13:47:27.413Z"
    val eligibleBookings = Bookings.eligibleForAnalysis(
      bookings,
      "2010-01-17T13:47:27.413Z",
      "2030-03-17T13:47:27.413Z",
      Map("AMS"->"Netherlands"))
    eligibleBookings.count() shouldBe 2
  }
}
