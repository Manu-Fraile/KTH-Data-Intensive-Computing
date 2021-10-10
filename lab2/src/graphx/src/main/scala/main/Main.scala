package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphLoader

object Main extends App{
  //Turn off red INFO logs
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("Hola Mundo!")

  val spark = SparkSession.builder()
    .appName("Spark-Demo")//assign a name to the spark application
    .master("local[*]") //utilize all the available cores on local
    .getOrCreate()

  val sparkConf = new SparkConf()
  val sc = spark.sparkContext

  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50)),
    (7L, ("Alex", 55))
  )

  val edgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(5L, 2L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3),
    Edge(7L, 5L, 3),
    Edge(7L, 6L, 4)
  )

  val vertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(vertexArray)
  val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
  val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

  case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
  val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }

  val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
    case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
  }.outerJoinVertices(initialUserGraph.outDegrees) {
    case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
  }

  q1(graph)
  q2(graph)
  q3(graph)
  q4(userGraph)
  q5(userGraph)
  q6(userGraph)

  def q1(graph: Graph[(String, Int), Int]): Unit ={
    println("\nTASK 1")

    graph.vertices
      .filter { case (id, (name, age)) => age >=30 }
      .map(v => s"${v._2._1} is ${v._2._2}")
      .collect().foreach(println)
  }

  def q2(graph: Graph[(String, Int), Int]): Unit={
    println("\nTASK 2")

    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
  }

  def q3(graph: Graph[(String, Int), Int]): Unit ={
    println("\nTASK 3")

    for (triplet <- graph.triplets.filter{ triplet => triplet.attr > 4}.collect) {
      println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
    }
  }

  def q4(userGraph: Graph[User, Int]): Unit = {
    println("\nTASK 4")

    userGraph.vertices
      .map(v => s"${v._2.name} is liked by ${v._2.inDeg} people.")
      .collect().foreach(println)
  }

  def q5(userGraph: Graph[User, Int]): Unit ={
    println("\nTASK 5")

    userGraph.vertices
      .filter{ case (id, us) => us.inDeg == us.outDeg}
      .map(v => s"${v._2.name}")
      .collect().foreach(println)
  }

  def q6(userGraph: Graph[User, Int]): Unit ={
    println("\nTASK 6")

    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
      triplet => triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age),
      (a, b) => { if (a._2 > b._2) a else b}
    )

    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str) }
  }
}
