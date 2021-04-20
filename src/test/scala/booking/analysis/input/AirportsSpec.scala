package booking.analysis.input

import booking.SparkSetup
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AirportsSpec extends AnyFlatSpec with Matchers with SparkSetup {

  "Airports data" should "be loaded correctly" in withSparkSession { spark =>
    val airportsFile = s"${os.pwd.toString}/src/test/resources/airports-sample.dat"
    val airports = Airports.toLookup(spark, airportsFile)
    airports.size shouldBe 1
    airports.get("AMS") shouldBe Some("Netherlands", "Europe/Amsterdam")
  }
}
