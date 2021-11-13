package com.binarray.dev.kafka.standalone.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * @author Ashesh
 */
public class BasicKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(BasicKafkaProducer.class);

    public static void main(String[] args) {
        // Step-01: Create Producer Properties
        var producerProperties = new Properties();

        // Option-01:
        //producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Option-02:
        // Load properties from a file.
        try(var inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka.properties")) {
            producerProperties.load(inputStream);
        } catch (IOException e) {
           logger.error("Error loading properties.", e);
        }
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step-02: Create Kafka Producer
        /* Using the try-with-resources will close the KafkaProducer which will actually
         * flush and close the KafkaProducer and produce the record.
         */
        try (var kafkaProducer = new KafkaProducer<String, String>(producerProperties)) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("binarray_dev_topic", "Hello from Java! " + new Date());

            // Step-03: Produce Messages
            kafkaProducer.send(producerRecord);
        }

        // If not using try-with-resources, use below code to flush and produce the data.
        // kafkaProducer.flush();
        // kafkaProducer.close();
    }
}
