package com.binarray.dev.kafka.standalone.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

/**
 * @author Ashesh
 */
public class CallbackKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(CallbackKafkaProducer.class);

    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092"; // TODO read from properties.

        // Step-01: Create Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step-02: Create Kafka Producer
        /* Using the try-with-resources will close the KafkaProducer which will actually
         * flush and close the KafkaProducer and produce the record.
         */
        try (var kafkaProducer = new KafkaProducer<String, String>(producerProperties)) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("binarray_dev_topic", "Hello from Java Callback! " + i);

                // Step-03: Produce Messages
                kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Error occurred.", e);
                    } else {
                        logger.info("Successfully produced.\n"
                                + "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "Offset: " + recordMetadata.offset() + "\n"
                                + "Date: " + new Date(recordMetadata.timestamp()) + "\n");
                    }
                });
            }
        }

        // If not using try-with-resources, use below code to flush and produce the data.
        // kafkaProducer.flush();
        // kafkaProducer.close();
    }
}
