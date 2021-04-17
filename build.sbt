name := "booking-analysis"
version := "0.1"
scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.2.7" % "test",
  "com.jsuereth" % "scala-arm_2.12" % "2.0",
  "com.lihaoyi" %% "upickle" % "0.9.5"
)

parallelExecution in Test := false