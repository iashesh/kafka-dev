package com.binarray.dev.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

/**
 * This is client class for connecting to elastic search and performing basic REST calls.
 *
 * @author Ashesh
 */
public class BasicElasticsearchClient {
    private static final Logger logger = LoggerFactory.getLogger(BasicElasticsearchClient.class);

    public static void main(String[] args) {
        var configProps = new Properties();
        try(var inputStream
                    = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("elasticsearch.properties")) {
            configProps.load(inputStream);
        } catch (IOException ioEx) {
            logger.error("Error loading properties.", ioEx);
        }
        String connString = configProps.getProperty("elasticsearch.url");

        URI connUri = URI.create(connString);
        String[] auth = connUri.getUserInfo().split(":");

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search.html
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse  = restClient.search(searchRequest, RequestOptions.DEFAULT);
            // Show that the query worked
            logger.info("Search Response: {}", searchResponse.toString());
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
        }
    }
}
