package com.github.omarmarie.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run(); // create an object ConsumerDemoWithThreads and launch the run method
    }

    private ConsumerDemoWithThreads(){}

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "localhost:9092";
        String groupId          = "my-sixth-application";
        String topic            = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch    = new CountDownLatch(1);

        // Create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught the shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown(); // allow shutdown consumer
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally{
            logger.info("Application is closing");
        }
    }

    // Create a new class for kafka consumer to do everything we need.
    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch; // count down latch deals with concurrency to shut down app correctly
        private KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        // Our Constructor
        public ConsumerRunnable(String bootstrapServers,
                               String groupId,
                               String topic,
                               CountDownLatch latch)
        {
            this.latch = latch;

            // Create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from very begging of topic

            this.consumer = new KafkaConsumer<String, String>(properties); // Create new consumer
            this.consumer.subscribe(Arrays.asList(topic));// subscribe consumer to our topic(s) | Can add another topics
        }

        @Override
        public void run() {
            // poll for new data | consumer doesn't get new data until it asks for that data
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); // new in kafka version 2.0.0 and up

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info(("Partition: " + record.partition() + " Offset: " + record.offset()));
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal!!!");
            } finally {
                consumer.close();
                // Tells our main code that we are done with the consumer
                latch.countDown();
            }
        }

        // Shutdown our consumer thread
        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the Exception WakeUpException
            consumer.wakeup();
        }

    }
}
