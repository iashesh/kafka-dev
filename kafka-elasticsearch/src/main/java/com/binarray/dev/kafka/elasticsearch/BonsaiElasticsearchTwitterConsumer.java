package com.binarray.dev.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This is twitter consumer and will upload tweets in Elastic search cloud.
 *
 * @author Ashesh
 */
public class BonsaiElasticsearchTwitterConsumer {
    private static final Logger logger = LoggerFactory.getLogger(BonsaiElasticsearchTwitterConsumer.class);
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
        RestHighLevelClient restClient = createElasticsearchRestClient();

        // Step-02: Create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(KAFKA_TOPIC);

        // Step-03: Consume from topic and Add to Elasticsearch
        boolean keepConsuming = true;
        int consumedTweetCount = 0;
        try {
            while(keepConsuming) {

                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("Record received. \n"
                            + "Partition: " + record.partition() + "\n"
                            + "Offset: " + record.offset());

                    IndexRequest indexRequest = new IndexRequest(ELASTICSEARCH_INDEX, ELASTICSEARCH_TYPE);
                    indexRequest.source(record.value(), XContentType.JSON);

                    IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("Record indexed. \n"
                            + "Id: " + indexResponse.getId());

                    // Add a delay to see what's printing on console
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

    /**
     * Create Kafka Consumer
     * @param topic
     * @return KafkaConsumer
     */
    private static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        // Step-01: Create Consumer Config
        Map<String, Object> consumerConfig =
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Step-02: Create Kafka Consumer
        var kafkaConsumer = new KafkaConsumer(consumerConfig);

        // Step-03: Subscribe to Topic
        kafkaConsumer.subscribe(List.of(topic));

        return kafkaConsumer;
    }

    /**
     * Create Elasticsearch Client.
     *
     * @return
     */
    private static RestHighLevelClient createElasticsearchRestClient() {
        RestHighLevelClient restClient = null;
        var configProps = new Properties();
        try(var inputStream
                    = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("properties/elasticsearch.properties")) {
            configProps.load(inputStream);
        } catch (IOException ioEx) {
            logger.error("Error loading properties.", ioEx);
        }
        if (configProps.getProperty("elasticsearch.url") != null) {
            String connectionString = configProps.getProperty("elasticsearch.url");

            URI connUri = URI.create(connectionString);
            String[] auth = connUri.getUserInfo().split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restClient;
    }
}
