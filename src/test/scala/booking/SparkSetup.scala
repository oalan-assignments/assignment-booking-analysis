package booking

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkSetup {
  def withSparkContext(testMethod: SparkContext => Any): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark test")
    val sparkContext = new SparkContext(conf)
    try {
      testMethod(sparkContext)
    }
    finally sparkContext.stop()
  }

  def withSparkSession(testMethod: SparkSession => Any): Unit = {
    val session = SparkSession
      .builder
      .appName("Spark test")
      .master("local")
      .getOrCreate()
    try {
      testMethod(session)
    }
    finally session.close()
  }
}
