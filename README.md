## Quick Deploy

1. Compile and assemble the project
```
$ sbt compile
$ sbt assembly
```
2. If running the producer remotely (i.e. not in the same VPC as the Kafka Cluster):
    - ssh into the Kafka master node and modify the following line in
    /app/kafka/kafka_2.12-0.11.0.0/config/server.properties:
    ```
    advertised.listeners=PLAINTEXT://<aws-public-dns>:9092
    ```
    - restart the service:
    ```
    nohup sudo /app/kafka/kafka_2.12-0.11.0.0/bin/kafka-server-start.sh /app/kafka/kafka_2.12-0.11.0.0/config/server.properties &
    ```

2. Execute the jar file:
```
java -jar target/scala-2.11/KafkaProducer-assembly-0.1.0-SNAPSHOT.jar <metadataBrokerList> <topic> <messagesPerSec> <wordsPerMessage>
```
3. Check that messages are being delivered by starting up a Kafka Consumer using the Kafka CLI on one of the cluster nodes:
```
/app/kafka/kafka_2.12-0.11.0.0/bin/kafka-console-consumer.sh --bootstrap-server <kafka-broker-ip>:9092 --topic <your-topic> --from-beginning
```