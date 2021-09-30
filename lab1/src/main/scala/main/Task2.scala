package main

import main.Main.spark
import org.apache.spark.sql.DataFrame

object Task2 extends App {

  //3. Compute the min, max, and average page size.
  def q3(data_log_df: DataFrame): Unit ={
    println("\nq3 solution: ")
    data_log_df.createOrReplaceTempView("pages")
    val sqlDF = spark.sql("SELECT MIN(page_size), MAX(page_size), AVG(page_size) FROM pages")
    sqlDF.show()
  }

  //5. Determine the record with the largest page size again. But now, pick the most popular.
  def q5(data_log_df: DataFrame): Unit ={
    println("\nq5 solution: ")
    data_log_df.createOrReplaceTempView("pages")
    val sqlDF = spark.sql("SELECT * FROM pages WHERE page_size = (SELECT MAX(page_size) FROM pages) ORDER BY page_hits DESC")
    sqlDF.show()
  }

  //7. Use the results of Question 3, and create a new RDD with the records that have greater page size than
  //the average.
  def q7(data_log_df: DataFrame): Unit ={
    println("\nq7 solution: ")
    data_log_df.createOrReplaceTempView("pages")
    val sqlDF = spark.sql("SELECT * FROM pages WHERE page_size > (SELECT AVG(page_size) FROM pages)")
    sqlDF.show()
  }

  //12. Determine the number of unique terms appearing in the page titles. Note that in page titles, terms
  //are delimited by \ " instead of a white space. You can use any number of normalization steps (e.g.,
  //lowercasing, removal of non-alphanumeric characters).
  def q12(data_log_df: DataFrame): Unit ={
    println("\nq12 solution: ")
    data_log_df.createOrReplaceTempView("pages")
    val sqlDF = spark.sql("SELECT COUNT(*) AS unique_terms FROM(SELECT DISTINCT explode(split(regexp_replace(LOWER(page_title), '[^A-Za-z0-9]', ' '), ' ')) FROM pages)")
    sqlDF.show()
  }

  //13. Determine the most frequently occurring page title term in this dataset.
  def q13(data_log_df: DataFrame): Unit ={
    println("\nq13 solution: ")
    data_log_df.createOrReplaceTempView("pages")
    val sqlDF = spark.sql("SELECT page_title, COUNT(*) AS Freq FROM pages GROUP BY page_title ORDER BY COUNT(*) DESC LIMIT 1")
    sqlDF.show()
  }
}
