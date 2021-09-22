package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main extends App {
  println("Hello, World!")

  val spark = SparkSession.builder().getOrCreate()

  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)
  val pagecounts = sc.textFile("../../../../../pagecounts-20160101-000000_parsed.out")
}
