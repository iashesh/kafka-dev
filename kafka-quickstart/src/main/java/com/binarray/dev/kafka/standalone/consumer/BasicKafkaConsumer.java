package com.binarray.dev.kafka.standalone.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * @author Ashesh
 */
public class BasicKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(BasicKafkaConsumer.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        // Step-01: Create Consumer Config
        Map<String, Object> consumerConfig =
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Step-02: Create Kafka Consumer
        try (var kafkaConsumer = new KafkaConsumer(consumerConfig)) {
            // Step-03: Subscribe to Topic
            kafkaConsumer.subscribe(List.of("binarray_dev_topic"));

            // Step-04: Poll
            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

                for(ConsumerRecord<String, String> record : records) {
                    logger.info("Record received. \n"
                            + "Key: " + record.key() + "\n"
                            + "Value: " + record.value() + "\n"
                            + "Partition: " + record.partition() + "\n"
                            + "Offset: " + record.offset());
                }
            }
        }
    }
}
