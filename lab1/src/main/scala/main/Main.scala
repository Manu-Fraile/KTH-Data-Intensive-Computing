package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main extends App{
  //Turn off red INFO logs
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //  val spark = SparkSession.builder().getOrCreate()
  //
  //  val sparkConf = new SparkConf()
  //  val sc = new SparkContext(sparkConf)
  //  val pagecounts = sc.textFile("../../../../../pagecounts-20160101-000000_parsed.out")

  val spark = SparkSession.builder()
    .appName("Spark-Demo")//assign a name to the spark application
    .master("local[*]") //utilize all the available cores on local
    .getOrCreate()
  import spark.implicits._

  val sparkConf = new SparkConf()
  // val sc = new SparkContext(sparkConf)
  val sc = spark.sparkContext
  val pagecounts = sc.textFile("pagecounts-20160101-000000_parsed.out")

  val data_log = pagecounts.map(row => row.split(' ')).map(field => Log(field(0), field(1), field(2).toInt, field(3).toLong))
  val data_log_df = data_log.toDF()

  println("TASK 1:")
  println()
  Task1.q1(data_log)
  Task1.q2(data_log)
  Task1.q3(data_log)
  Task1.q4(data_log)
  Task1.q5(data_log)
  Task1.q6(data_log)
  Task1.q7(data_log)
  Task1.q8(data_log)
  Task1.q9(data_log)
  Task1.q10(data_log)
  Task1.q11(data_log)
  Task1.q12(data_log)
  Task1.q13(data_log)

  println()
  println("\nTASK 2:")
  println()
  Task2.q3(data_log_df)
  Task2.q5(data_log_df)
  Task2.q7(data_log_df)
  Task2.q12(data_log_df)
  Task2.q13(data_log_df)

}
