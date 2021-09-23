package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main extends App {
  println("Hello, World!")

//  val spark = SparkSession.builder().getOrCreate()
//
//  val sparkConf = new SparkConf()
//  val sc = new SparkContext(sparkConf)
//  val pagecounts = sc.textFile("../../../../../pagecounts-20160101-000000_parsed.out")

  val spark = SparkSession.builder()
                .appName("Spark-Demo")//assign a name to the spark application
                .master("local[*]") //utilize all the available cores on local
                .getOrCreate()

  println("Hello, World 2!")
  val sparkConf = new SparkConf()
  // val sc = new SparkContext(sparkConf)
  val sc = spark.sparkContext
  val pagecounts = sc.textFile("../../../../../pagecounts-20160101-000000_parsed.out")
}
