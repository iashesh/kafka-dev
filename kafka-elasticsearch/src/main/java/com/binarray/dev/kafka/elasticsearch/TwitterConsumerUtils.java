package com.binarray.dev.kafka.elasticsearch;

import com.binarray.dev.kafka.common.FileUtils;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Twitter consumer utils.
 *
 * @author Ashesh
 */
public class TwitterConsumerUtils {
    private static final Logger logger = LoggerFactory.getLogger(TwitterConsumerUtils.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String GROUP_ID = "binarray-twitter-consumer";

    /**
     * Create Kafka Consumer
     * @param topic kafka topic
     * @return KafkaConsumer
     */
    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        // Step-01: Create Consumer Config
        Map<String, Object> consumerConfig =
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Step-02: Create Kafka Consumer
        var kafkaConsumer = new KafkaConsumer<String, String>(consumerConfig);

        // Step-03: Subscribe to Topic
        kafkaConsumer.subscribe(List.of(topic));

        return kafkaConsumer;
    }

    /**
     * Create Elasticsearch Client.
     *
     * @return RestHighLevelClient
     */
    public static RestHighLevelClient createElasticsearchRestClient() {
        RestHighLevelClient restClient = null;
        var configProps = FileUtils.loadProperties("properties/elasticsearch.properties");

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

    /**
     * Retrieves and return key for the tweet.
     *
     * @param tweetJson tweet JSON string.
     * @return key for the tweet.
     */
    public static String getKeyForTweet(String tweetJson) {
        String key = null;
        try {
            key = JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("id_str")
                    .getAsString();
        } catch (Exception ex) {
            logger.error("Error occurred while getting key for tweet. Tweet: {}", tweetJson );
        }
        return key;
    }
}
