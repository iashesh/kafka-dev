package com.binarray.dev.kafka.standalone.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
public class AssignAndSeekKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AssignAndSeekKafkaConsumer.class);

    public static void main(String[] args) {
        // Step-01: Create Consumer Config
        Map<String, Object> consumerConfig =
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                    );

        // Step-02: Create Kafka Consumer
        try (var kafkaConsumer = new KafkaConsumer(consumerConfig)) {
            // Step-03: Assign a Topic Partition
            TopicPartition partitionAssigned =  new TopicPartition("binarray_dev_topic", 0);
            kafkaConsumer.assign(List.of(partitionAssigned));

            // Step-04: See a Partition Offset
            long offsetToReadFrom = 65;
            kafkaConsumer.seek(partitionAssigned, offsetToReadFrom);

            // Step-04: Poll and Read only 5 messages
            boolean continueReading = true;
            int recordsRead = 0;
            while(continueReading) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

                for(ConsumerRecord<String, String> record : records) {
                    recordsRead += 1;
                    logger.info("Record #" + recordsRead + " received. \n"
                            + "Key: " + record.key() + "\n"
                            + "Value: " + record.value() + "\n"
                            + "Partition: " + record.partition() + "\n"
                            + "Offset: " + record.offset());

                    // Exit loop condition
                    if (recordsRead >= 5) {
                        continueReading = false;
                        break;
                    }
                }
            }
        }
    }
}
