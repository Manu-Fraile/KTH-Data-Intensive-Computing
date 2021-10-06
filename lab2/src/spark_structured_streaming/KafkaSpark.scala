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
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

object KafkaSpark {
  def main(args: Array[String]) {

    //Turn off red INFO logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("AVG")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("checkpoint")
    
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
    val topics = Set("avg")
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaConf, topics)

    val pairs = messages.map(x => {
      val s = x._2.split(",")
      (s(0), s(1).toDouble)
    })


    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double)]): (String, Double) = {
      if (state.exists()) {

        val (count, mean) = state.get()
        val newCount = count + 1
        val differential = (value.get - mean) / newCount
        val newMean = mean + differential
        val newState = (newCount, newMean)
        state.update(newState)

        (key, newMean)

      } else {
        state.update(1, value.get)
        (key, value.get)
      }
    }

    // Attach the mapping function
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)).map{case (key, mean) => {
      (key, mean)
    }}
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
