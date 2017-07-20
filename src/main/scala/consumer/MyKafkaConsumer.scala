package consumer

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: MyKafkaConsumer <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.MyKafkaConsumer broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object MyKafkaConsumer {
  def main(args: Array[String]) {

    // log settings
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

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
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MyKafkaConsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    ssc.checkpoint("checkpoints")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get lines
    val lines = messages.map(_._2)

    // Get words
    val words = lines.flatMap(_.split(" "))

    // get top visited sites in past 60 seconds
    val topCounts60 = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(ascending = false))
//    topCounts60.print()  // DEBUG

    // Print popular sites
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular sites in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s visits)".format(tag, count)) }
    })
//    topCounts60.saveAsTextFiles("result")


    lines.count().map(cnt => "Received " + cnt + " kafka messages.").print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
