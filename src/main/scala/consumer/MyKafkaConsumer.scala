package consumer

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import scala.io.Source
import org.apache.avro.Schema
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory


/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: MyKafkaConsumer <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.MyKafkaConsumer broker1-host:port,broker2-host:port \
  *    topic1,topic2
  *
  * Refs:
  * - http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html
  * - https://blog.knoldus.com/2016/08/02/scala-kafka-avro-producing-and-consuming-avro-messages/
  *
  * */
object MyKafkaConsumer {

  object Injection {
    //reads avro schema file and Initializes schema
    val schemaString: String = Source.fromURL(getClass.getResource("/schema.avsc")).mkString
    val schema: Schema = new Schema.Parser().parse(schemaString)
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
  }

  // log settings
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("spark").setLevel(Level.ERROR)


  def main(args: Array[String]) {
    println(">>> Starting Kafka Consumer...")

    if (args.length < 2) {
      System.err.println(
        s"""
        |Usage: MyKafkaConsumer <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 10 second batch interval
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MyKafkaConsumer")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    ssc.checkpoint("checkpoints")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    // Get lines
    messages.foreachRDD(rdd => {
      rdd.foreach({ avroRecord =>

        // Deserialize and create generic record
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(avroRecord, null)
        val data: GenericRecord = Injection.reader.read(null, decoder)
        println(s">>> Got data: $data")
//
//        // Get words froms sites string
//        val sites = List(data.get("sites").toString)
//        val words = sites.flatMap(_.split(" ")).map(x => (x, 1L))
//        println(words)


//        val words = sites.flatMap(_.split(" "))
//
        // get top visited sites in past 60 seconds
//        val topCounts60 = words.reduceByKeyAndWindow(_ + _, Seconds(60))
//          .map { case (topic, count) => (count, topic) }
//          .transform(_.sortByKey(ascending = false))
//        // topCounts60.print()  // DEBUG
//
//        // Print popular sites
//        topCounts60.foreachRDD(rdd => {
//          val topList = rdd.take(10)
//          println("\nPopular sites in last 60 seconds (%s total):".format(rdd.count()))
//          topList.foreach { case (count, tag) => println("%s (%s visits)".format(tag, count)) }
//        })
//        // topCounts60.saveAsTextFiles("result")
//
//
//        lines.count().map(cnt => "Received " + cnt + " kafka messages.").print()
      })
    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
