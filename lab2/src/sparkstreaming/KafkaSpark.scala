package sparkstreaming

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
    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("AVG")
    val ssc = new StreamingContext(conf, Seconds(1))
    //<FILL IN>
    
    val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("avg")
    val messages = KafkaUtils.createDirectStream(ssc, kafkaConf, topics)
    //<FILL IN>

    // measure the average value for each key in a stateful manner
    //def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	//<FILL IN>
    //}
    //val stateDstream = pairs.mapWithState(<FILL IN>)

    messages.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
