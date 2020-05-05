/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka_consumer_elasticsearch;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author microchel
 */
public class ElasticSearchConsumer {
    
    public static RestHighLevelClient createClient()
    {
        String hostname = "hello-brush-2080710813.us-west-2.bonsaisearch.net";
        String username = "4a3k5lyn3v";
        String password = "56mn2ua53x";
        
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, 
                new UsernamePasswordCredentials(username, password));
        
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder hacb) {
                return hacb.setDefaultCredentialsProvider(credentialsProvider);
            }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    
    public static KafkaConsumer<String, String> createConsumer(String topic){
        
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        
        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit

        
        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties); 
        consumer.subscribe(Arrays.asList(topic));       
        
        return consumer;    
    }
    
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson)
    {
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
    
    public static void main(String[] args) throws IOException {
        
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        
        RestHighLevelClient client = createClient();
                        
        KafkaConsumer<String, String> consumer = createConsumer("tweets_1");
        
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));  
            
            int recordsCount = records.count();
            logger.info("Received: " + recordsCount + " records");
            
            BulkRequest bulkRequest = new BulkRequest();
            
            for(ConsumerRecord<String, String> record : records)
            {
                // insert data into elasticsearch
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                try{
                    String id = extractIdFromTweet(record.value());
                
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter_2",
                            "tweets",
                            id // this is to make consumer idempotent
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }catch(NullPointerException ex){
                    logger.warn("Skipping bad data: " + record.value());
                }

//                IndexResponse indexResponce = client.index(indexRequest, RequestOptions.DEFAULT);
//                logger.info(indexResponce.getId());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(ElasticSearchConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
            if(recordsCount > 0)
            {
                BulkResponse bulkItemResponce = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Commiting offsets");
                consumer.commitSync();
                logger.info("Offset have been commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(ElasticSearchConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }

            }
        }
        
        // client.close();
    }
}
