/**
  * Created by KANUGOA on 7/12/2017.
  */
import java.nio.ByteBuffer
import java.util.HashMap
import scala.util.Random
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
//import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
object spark_streaming {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    System.setProperty("hadoop.home.dir", "C:\\winutil")
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = SparkSession.builder().master("local[*]").appName("KafkaOrderStatus").getOrCreate()
    val ssc = new StreamingContext(sparkConf.sparkContext, Seconds(1))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // create kafka stream from input topic
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val status_count = lines.map(line => line.split(",")(2)).map(x => (x, 1)).reduceByKey(_ + _)
    // Write the results to kafka output topic
    status_count.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val props = setProperties()
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val data = record.toString()
          val message = new ProducerRecord[String, String]("testout", null, data)
          producer.send(message)
        })
        producer.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
  // set spark streaming context and create Dtream from kafka producer
  def setSparkStreaming(args: Array[String]): DStream[String]={
    System.setProperty("hadoop.home.dir","C:\\winutil")
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaOrderStatus")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // create kafka stream from input topic
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines
  }

  // set properties for producer class for kafka
  def setProperties(): HashMap[String, Object] ={
    val kafkaTopic = "testout"
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}
