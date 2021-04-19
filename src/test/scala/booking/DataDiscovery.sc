import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder
  .appName("booking-analysis")
  .config(new SparkConf())
  .master("local[1]")
  .getOrCreate()






//  .sortBy(_._2.noOfPassengers)
//  .write.csv("result")









