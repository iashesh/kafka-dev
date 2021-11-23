package com.binarray.dev.kafka.twitter;


import com.binarray.dev.kafka.common.FileUtils;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Stream and produce twitter posts to a Kafka topic.
 *
 * @author Ashesh
 */
public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
       new TwitterProducer().produce();

       Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Exiting application.")));
    }

    /**
     * Method to stream and produce tweets to kafka topic.
     */
    private void produce() {
        Client client = null;
        KafkaProducer kafkaProducer = null;
        ProducerRecord<String, String> producerRecord;
        try {
            /* Set up your blocking queues:
             * Be sure to size these properly based on expected TPS (Transactions Per Second) of your stream.
             */
            BlockingQueue<String> tweetsQueue = new LinkedBlockingQueue<>(1000);
            // Create twitter client
            client = createHosebirdClient(tweetsQueue);
            // Attempts to establish a connection.
            client.connect();

            // Create Kafka producer
            kafkaProducer = createKafkaProducer();

            // Send tweets to Kafka Topic
            while (!client.isDone()) {
                String strTweet = tweetsQueue.poll(5, TimeUnit.SECONDS);
                if (strTweet != null) {
                    logger.info("Tweet JSON: " + strTweet);
                    producerRecord = new ProducerRecord<>("twitter_kafka_topic", strTweet);
                    kafkaProducer.send(producerRecord, (recordMetadata, ex) -> {
                        if (ex != null) {
                            logger.error("Error occurred.", ex);
                        } else {
                            logger.info("Tweet successfully produced.\n"
                                    + "Topic: " + recordMetadata.topic() + "\n"
                                    + "Partition: " + recordMetadata.partition() + "\n"
                                    + "Offset: " + recordMetadata.offset() + "\n"
                                    + "Date: " + new Date(recordMetadata.timestamp()) + "\n");
                        }
                    });
                }
            }
        } catch (Exception ex) {
            logger.error("ERROR: ERROR: ERROR:", ex);
        } finally {
            if (client != null) client.stop();
            if (kafkaProducer != null) kafkaProducer.close();
        }
    }

    /**
     * Create a hosebird client for streaming twitter tweets.
     *
     * @param tweetsQueue tweets queue
     * @return Client object.
     */
    private Client createHosebirdClient(BlockingQueue<String> tweetsQueue) {
        Client hosebirdClient = null;
        Properties twitterConfig = FileUtils.loadProperties("twitter.properties");
        /** Declare the host you want to connect to, the endpoint,
         * and authentication (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        //List<Long> followings = List.of(1234L, 566788L);          // Search People
        //hosebirdEndpoint.followings(followings);

        List<String> terms = List.of("kafka", "confluent", "bitcoin");   // Search Terms
        //List<String> terms = List.of("binarray");     // Go and tweet about some specific keywords.
        hosebirdEndpoint.trackTerms(terms);

        // Read secrets from a config file.
        Authentication hosebirdAuth = new OAuth1(twitterConfig.getProperty(TwitterConstants.TW_API_KEY_CONFIG),
                    twitterConfig.getProperty(TwitterConstants.TW_API_SECRET_CONFIG),
                    twitterConfig.getProperty(TwitterConstants.TW_API_ACCESS_TOKEN_CONFIG),
                    twitterConfig.getProperty(TwitterConstants.TW_API_ACCESS_SECRET_CONFIG));

        // Creating a hosebird client
        ClientBuilder builder = new ClientBuilder()
                .name("Binarray-Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(tweetsQueue));

        hosebirdClient = builder.build();
        return hosebirdClient;
    }

    /**
     * Create a KafkaProducer instance.
     *
     * @return KafkaProducer
     */
    private KafkaProducer createKafkaProducer() {
        KafkaProducer kafkaProducer =null;
        Properties twitterConfig = FileUtils.loadProperties("kafka.properties");
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, twitterConfig.getProperty("bootstrap.servers"));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        return kafkaProducer;
    }
}
