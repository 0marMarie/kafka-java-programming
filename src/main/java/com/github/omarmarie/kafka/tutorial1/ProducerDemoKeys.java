package com.github.omarmarie.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        // Create a Logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServer = "localhost:9092";

        // 1. Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());   // Help the producer know what type of value you are sending to kafka
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Help the producer know what type of value you are sending to kafka

        // 2. Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        // Loop to produce multiple messages
        for (int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key   = "id_" + Integer.toString(i); // add a key with every topic

            // Create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value );

            // 3. Send Data - asynchronous
            Thread.sleep(1000); // Added before sending new messages due the Sticky Partitioning strategy Kafka has

            logger.info("Key: " + key); // log the key
            // id_0 - partition 1
            // id_1 - partition 0
            // id_2 - partition 2
            // id_3 - partition 0
            // id_4 - partition 2
            // id_5 - partition 2
            // id_6 - partition 0
            // id_7 - partition 2
            // id_8 - partition 1
            // id_9 - partition 2

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // This is executed every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // The record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp());
                    } else {
                        // log the exception
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block .send() to make it synchronous - don't do it in production !!!!
        }

        producer.flush();        // flush data
        producer.close();        // flush and close producer

    }
}
