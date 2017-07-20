package com.sparkdeveloper.receiver

import java.io.ByteArrayOutputStream
import java.util.HashMap
import org.apache.avro.SchemaBuilder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import kafka.serializer._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.{ByteArrayOutputStream, File, IOException}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io._
import org.apache.avro.SchemaBuilder
import org.apache.avro.Schema
import org.apache.avro._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import com.databricks.spark.avro._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

case class HashtagEntities(text: String, start: Double, end: Double)

case class User(id: Double, name: String,
                screenName: String, location: String, description: String, url: String, statusesCount: Double)

case class Tweet(text: String, createdAt: String, lang: String, source: String, expandedURL: String,
                 url: String, screenName: String, description: String, name: String, retweetCount: Double, timestamp: Long,
                 favoriteCount: Double, user: Option[User], hashtags: HashtagEntities)


/**
  * Created by timothyspann
  */
object KafkaConsumer {
  val tweetSchema = SchemaBuilder
    .record("tweet")
    .fields
    .name("tweet").`type`().stringType().noDefault()
    .name("timestamp").`type`().longType().noDefault()
    .endRecord

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val logger: Logger = Logger.getLogger("com.sparkdeveloper.receiver.KafkaConsumer")
    val sparkConf = new SparkConf().setAppName("Avro to Kafka Consumer")

    sparkConf.set("spark.cores.max", "24") // For my sandbox
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.set("spark.sql.tungsten.enabled", "true")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.app.id", "KafkaConsumer") // want to know your app in the UI
    sparkConf.set("spark.io.compression.codec", "snappy")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")

    sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
    sparkConf.set("spark.sql.parquet.mergeSchema", "true")
    sparkConf.set("spark.sql.parquet.binaryAsString", "true")

    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    val ssc = new StreamingContext(sc, Seconds(2))

    try {
      val kafkaConf = Map(
        "metadata.broker.list" -> "sandbox.hortonworks.com:6667",
        "zookeeper.connect" -> "sandbox.hortonworks.com:2181", // Default zookeeper location
        "group.id" -> "KafkaConsumer",
        "zookeeper.connection.timeout.ms" -> "1000")

      val topicMaps = Map("meetup" -> 1)

      // Create a new stream which can decode byte arrays.
      val tweets = KafkaUtils.createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder]
      (ssc, kafkaConf,topicMaps, StorageLevel.MEMORY_ONLY_SER)

      try {
        tweets.foreachRDD((rdd, time) => {
          if (rdd != null) {
            try {
              val sqlContext = new SQLContext(sc)
              import sqlContext.implicits._

              val rdd2 = rdd.map { case (k, v) => parseAVROToString(v) }

              try {
                val result = rdd2.mapPartitions(records => {
                  val mapper = new ObjectMapper()
                  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                  mapper.registerModule(DefaultScalaModule)
                  records.flatMap(record => {
                    try {
                      Some(mapper.readValue(record, classOf[Tweet]))
                    } catch {
                      case e: Exception => None;
                    }
                  })
                }, true)

                val df1 = result.toDF()
                logger.error("Registered tweets: " + df1.count())
                df1.registerTempTable("tweets")

                // To show how easy it is to write multiple formats
                df1.write.format("orc").mode(org.apache.spark.sql.SaveMode.Append).orc("orcresults")
                df1.write.format("avro").mode(org.apache.spark.sql.SaveMode.Append).avro("avroresults")
                df1.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Append).parquet("parquetresults")
                df1.write.format("json").mode(org.apache.spark.sql.SaveMode.Append).json("jsonresults")
              } catch {
                case e: Exception => None;
              }
            }
            catch {
              case e: Exception => None;
            }
          }
        })
      } catch {
        case e: Exception =>
          println("Writing files after job. Exception:" + e.getMessage);
          e.printStackTrace();
      }
    } catch {
      case e: Exception =>
        println("Kafka Stream. Writing files after job. Exception:" + e.getMessage);
        e.printStackTrace();
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def parseAVROToString(rawTweet: Array[Byte]): String = {
    try {
      if (rawTweet.isEmpty) {
        println("Rejected Tweet")
        "Empty"
      }
      else {
        deserializeTwitter(rawTweet).get("tweet").toString
      }
    } catch {
      case e: Exception =>
        println("Exception:" + e.getMessage);
        "Empty"
    }
  }

  def deserializeTwitter(tweet: Array[Byte]): GenericRecord = {
    try {
      val reader = new GenericDatumReader[GenericRecord](tweetSchema)
      val decoder = DecoderFactory.get.binaryDecoder(tweet, null)
      reader.read(null, decoder)
    } catch {
      case e: Exception => None;
        null;
    }
  }
}
// scalastyle:on println

//build.sbt
//
//name := "KafkaConsumer"
//version := "1.0"
//scalaVersion := "2.10.6"
//jarName in assembly := "kafkaconsumer.jar"
//libraryDependencies  += "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided"
//libraryDependencies  += "org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "provided"
//libraryDependencies  += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided"
//libraryDependencies  += "com.databricks" %% "spark-avro" % "2.0.1"
//libraryDependencies  += "org.apache.avro" % "avro" % "1.7.6" % "provided"
//libraryDependencies  += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
//libraryDependencies  += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13"
//libraryDependencies  += "com.google.code.gson" % "gson" % "2.3"
//
//mergeStrategy in assembly := {
//  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
//  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
//  case "log4j.properties"                                  => MergeStrategy.discard
//  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
//  case "reference.conf"                                    => MergeStrategy.concat
//  case _                                                  => MergeStrategy.first
//}