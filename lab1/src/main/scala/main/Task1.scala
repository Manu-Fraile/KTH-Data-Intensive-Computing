package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Task1 extends App {

  def q1(data_log: RDD[Log]): Unit ={
    println("\nq1 solution: ")
    data_log.take(15).foreach(println)
  }

  //2. Determine the number of records the dataset has in total
  def q2(data_log: RDD[Log]): Unit ={
    println("\nq2 solution: ")
    println(data_log.count())
  }

  //3. Compute the min, max, and average page size.
  def q3(data_log: RDD[Log]): Unit ={
    println("\nq3 solution: ")
    var page_sizes = data_log.map(_.page_size)
    var min = page_sizes.min()
    var max = page_sizes.max()
    var avg = page_sizes.sum() / page_sizes.count()
    println(s"$min, $max, $avg")
  }

  //4. Determine the record(s) with the largest page size. If multiple records have the same size, list all of them.
  def q4(data_log: RDD[Log]): Unit ={
    println("\nq4 solution: ")
    val page_sizes = data_log.map(_.page_size)
    val max_size = page_sizes.max()
    data_log.filter(item => item.page_size.equals(max_size)).collect().foreach(println)
  }

  //5. Determine the record with the largest page size again. But now, pick the most popular.
  def q5(data_log: RDD[Log]): Unit ={
    println("\nq5 solution: ")
    var page_sizes = data_log.map(_.page_size)
    var page_hits = data_log.map(_.page_hits)
    var max_size = page_sizes.max()
    var max_hits = page_hits.max()
    data_log.filter(item => (item.page_size.equals(max_size) && item.page_hits.equals(max_hits))).collect().take(1).foreach(println)
  }

  //6. Determine the record(s) with the largest page title. If multiple titles have the same length, list all of them.
  def q6(data_log: RDD[Log]): Unit ={
    println("\nq6 solution: ")
    val page_titles = data_log.map(_.page_title.length)
    val largest_title = page_titles.max()
    data_log.filter(item => item.page_title.length.equals(largest_title)).collect().foreach(println)
  }

  //7. Use the results of Question 3, and create a new RDD with the records that have greater page size than
  //the average.
  def q7(data_log: RDD[Log]): Unit ={
    println("\nq7 solution: ")
    var page_sizes = data_log.map(_.page_size)
    var avg = page_sizes.sum() / page_sizes.count()
    var greater_avg = data_log.filter(item => item.page_size > avg).collect()
    greater_avg.foreach(println)
  }

  //8. Compute the total number of pageviews for each project.
  def q8(data_log: RDD[Log]): Unit ={
    println("\nq8 solution: ")
    data_log.groupBy(_.project_code).mapValues(_.map(_.page_hits).sum).foreach(println)
  }

  //9. Report the 10 most popular pageviews of all projects, sorted by the total number of hits.
  def q9(data_log: RDD[Log]): Unit ={
    println("\nq9 solution: ")
    var ten_most_popular = data_log.sortBy(item => item.page_hits, false).take(10).foreach(println)
  }

  //11. Determine the percentage of pages that have only received a single page view in this one hour of log
  //data.
  def q11(data_log: RDD[Log]): Unit ={
    println("\nq11 solution: ")
    var num_one_view_pages = data_log.filter(item => item.page_hits == 1).collect().size
    var size = data_log.count()
    println((100 * num_one_view_pages) / size + "%")
  }

  //13. Determine the most frequently occurring page title term in this dataset.
  def q13(data_log: RDD[Log]): Unit ={
    println("\nq13 solution: ")
    data_log.groupBy(item => item.page_title).mapValues(_.size).sortBy(_._2, false).take(1).foreach(println)
  }
}
