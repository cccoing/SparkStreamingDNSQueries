package producer

import java.util
import sys.process._
import scala.collection.mutable.ListBuffer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


object MyKafkaProducer {

  def main(args: Array[String]) {
    println(">>> Starting Kafka Producer...")

    if (args.length < 5) {
      System.err.println("Usage: KafkaProducer <metadataBrokerList> <topic> <minBatchSize> <messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val tShark_version = "which tshark" !

    if (tShark_version != 0) {
      println("tShark does not appear to be installed! Exiting...")
      System.exit(1)
    }

    val Array(brokers, topic, minBatchSize, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    props.put(ProducerConfig.RETRIES_CONFIG, "2")
    props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    props.put(ProducerConfig.ACKS_CONFIG, "all")

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while (true) {
      // get batch
      val batch = get_batch(minBatchSize.toInt)
      println("Got batch: ", batch)

      // send batch
      var i = 0
      var str = new ListBuffer[String]()

      for (entry <- batch) {
        str += entry
        i += 1

        if (i % wordsPerMessage.toInt == 0 || i == batch.length) {
          val message = new ProducerRecord[String, String](topic, null, str.toList.mkString(" "))
          producer.send(message)
          str.clear()

          println(s"Sent message: $message")
          Thread.sleep(messagesPerSec.toInt * 1000)
        }
      }
    }
  }


  def get_batch(batchsize: Int): Seq[String] = {
    var batch = new ListBuffer[String]()

    while (batch.length < batchsize) {

      // grab some DNS traffic
      val cmd = Seq("tshark", "-I", "-i", "en0", "-Y", "dns", "-c", "1000")

      val out = cmd.!!
        .split("\n")
        .toSeq
        .map(_.trim)
        .filter(_ != "")
        .filter(out => out.contains("Standard query response"))

      // filter out dns queries
      val pattern = """Standard query response\s\S*\s\S*\s(\S*\.\S*) """.r
      for(line <- out) {
        val site = pattern.findAllIn(line)
        if (site.nonEmpty) {
          batch += site.group(1)
        }
      }
    }

    //return
    batch.toList
  }

}
