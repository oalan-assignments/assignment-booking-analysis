name := "booking-analysis"
version := "0.1"
scalaVersion := "2.12.12"
assemblyJarName in assembly := "booking-analysis-0.1.jar"

val sparkDependencyScope = sys.props.getOrElse("sparkDependencyScope", default = "compile")

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-sql" % "3.1.1" % sparkDependencyScope,
"org.scalatest" %% "scalatest" % "3.2.7" % "test",
"com.lihaoyi" %% "upickle" % "0.9.5",
"com.lihaoyi" %% "os-lib" % "0.7.3",
)

parallelExecution in Test := false
