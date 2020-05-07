/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.kafka_producer_twitter;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author microchel
 */
public class TwitterProducer {
    
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    
    private String consumerKey;
    private String consumerSecret;
    
    private String accessToken;
    private String accessTokenSecret;
    
    private String bootstrapServer = "127.0.0.1:9092";
    
    public TwitterProducer(){
        Gson gson = new Gson(); // Creates new instance of Gson
        JsonElement element = null;
        try{
            element = gson.fromJson (new FileReader("/home/microchel/Service/twitter_config.json"), JsonElement.class); //Converts the json string to JsonElement without POJO 
        }catch(FileNotFoundException exc)
        {
            logger.error("File not found: ", exc);
        }
        
        JsonObject jsonObj = element.getAsJsonObject(); //Converting JsonElement to JsonObject

        consumerKey = jsonObj.get("consumer_key").getAsString(); 
        consumerSecret = jsonObj.get("consumer_secret").getAsString();
        
        accessToken = jsonObj.get("access_token").getAsString();
        accessTokenSecret = jsonObj.get("access_token_secret").getAsString();
    }
    
    public static void main(String[] args) {
        new TwitterProducer().run();
    }
    
    public void run()
    {
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(50);
        //create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();
        
        // create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
              
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            client.stop();
            producer.close();
        }));
        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }catch(InterruptedException ex)
            {
                ex.printStackTrace();
                client.stop();
            }
            if(msg != null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>("tweets_1", null, msg), new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null) {
                            logger.error("Something bad happened", exception);
                        }
                    }
                });
            }
        }
        logger.info("\nEnd of app");
    }
    
    
    
    public Client createTwitterClient(BlockingQueue<String> msgQueue)
    {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("ukraine", "bitcoin", "kafka", "microsoft", "google");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        
        ClientBuilder builder = new ClientBuilder()
            .name("client_sample")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    
    public KafkaProducer<String, String> createKafkaProducer()
    {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // create SAFE Producer
        kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        
        // setting high throughput producer (expence of CPU usage and latency)
        kafkaProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        kafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        kafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size
        
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);
        return producer;
    }
    
    
}
