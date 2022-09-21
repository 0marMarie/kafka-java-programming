package com.github.omarmarie.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";

        // 1. Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());   // Help the producer know what type of value you are sending to kafka
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Help the producer know what type of value you are sending to kafka

        // 2. Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // 4. Send Data - asynchronous
        producer.send(record);   // This is asynchronous
        producer.flush();        // flush data
        producer.close();        // flush and close producer

    }
}
