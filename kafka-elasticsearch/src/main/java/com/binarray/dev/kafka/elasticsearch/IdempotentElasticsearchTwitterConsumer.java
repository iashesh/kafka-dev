package com.binarray.dev.kafka.elasticsearch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * This is idempotent Twitter consumer and will upload tweets in Elastic search cloud using an ID.
 *
 * @author Ashesh
 */
public class IdempotentElasticsearchTwitterConsumer {
    private static final Logger logger = LoggerFactory.getLogger(IdempotentElasticsearchTwitterConsumer.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String GROUP_ID = "binarray-twitter-consumer";
    private static final String KAFKA_TOPIC = "twitter_kafka_topic";
    private static final String ELASTICSEARCH_INDEX = "twitter_dev";
    /*
     Latest Elasticsearch at Bonsai issues warning when type is provided in request,
     however API errors out when not. So using the type that Elasticsearch uses by default.
     */
    private static final String ELASTICSEARCH_TYPE = "_doc";
    private static final int TWEET_CONSUME_LIMIT = 100;

    public static void main(String[] args) {
        // Step-01: Create REST Client
        RestHighLevelClient restClient = TwitterConsumerUtils.createElasticsearchRestClient();

        // Step-02: Create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = TwitterConsumerUtils.createKafkaConsumer(KAFKA_TOPIC);

        // Step-03: Consume from topic and Add to Elasticsearch
        boolean keepConsuming = true;
        int consumedTweetCount = 0;
        try {
            while(keepConsuming) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("Record received from Kafka topic. \n"
                            + "Partition: " + record.partition() + "\n"
                            + "Offset: " + record.offset());

                    // Generate keys for the record (comment the line for either approaches)
                    String recordKey = null;
                    // Option-1: Create key using the record (topic, partition, offset)
                    //recordKey = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Option-2: Create key by extracting key from record (if exists)
                    recordKey = TwitterConsumerUtils.getKeyForTweet(record.value());
                    if (recordKey != null) {
                        IndexRequest indexRequest = new IndexRequest(ELASTICSEARCH_INDEX, ELASTICSEARCH_TYPE, recordKey);
                        indexRequest.source(record.value(), XContentType.JSON);

                        IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info("Record indexed in Elasticsearch. \n"
                                + "Id: " + indexResponse.getId());

                        // Added a delay to see what's printing on console
                        Thread.sleep(1000);

                        // Exit after max limit number of tweets
                        consumedTweetCount += 1;
                        if (consumedTweetCount == TWEET_CONSUME_LIMIT) {
                            keepConsuming = false;
                            logger.info("Max limit reached. Exiting now. \n");
                            break;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            // Log the exception
            logger.error("Error in ElasticSearch TwitterConsumer.", ex);
        } finally {
            // Need to close the client so the thread will exit
            try {
                restClient.close();
            } catch (IOException ioEx) {
                logger.error("Error in closing restClient.", ioEx);
            }
            // Close Kafka Consumer
            kafkaConsumer.close();
        }
    }
}
