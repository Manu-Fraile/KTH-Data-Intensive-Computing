package sparkstreaming

import org.apache.log4j.{Level, Logger}
import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.sql.streaming._

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object KafkaSpark {
  def main(args: Array[String]) {

    //Turn off red INFO logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("AVG")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val schema = StructType(Array(StructField("key", StringType), StructField("value", DoubleType)))
    case class Pair(key:String, value:Double)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092, localhost:2181")
      .option("subscribe", "avg")
      .load()
    val df_parsed = df.selectExpr("CAST(value AS STRING)").as[(String)]

    //create new columns, parse out the orig message and fill column with the values
    val df_schema = df_parsed.selectExpr("value",
                                "split(value,',')[0] as key",
                                "split(value,',')[1] as value1")
                                      .drop("value");
    //val df_schema = df_parsed.map(row => row.split(',')).map(field => Pair(field(0), field(1).toDouble))

    
    df_schema.createOrReplaceTempView("dfView")
    val res = spark.sql("SELECT key, AVG(value1) AS avg FROM dfView GROUP BY key") // returns another streaming DF
    
    res.writeStream
    .trigger(Trigger.ProcessingTime("1 second"))
    .option("checkpointLocation", "checkpoint")
    .outputMode("complete")
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()
  }
}
