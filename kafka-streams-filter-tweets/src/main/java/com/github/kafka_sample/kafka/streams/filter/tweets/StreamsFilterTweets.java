/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.kafka_sample.kafka.streams.filter.tweets;


import com.google.gson.JsonParser;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/**
 *
 * @author microchel
 */
public class StreamsFilterTweets {
    
    private static JsonParser jsonParser = new JsonParser();
    
    private static Integer extractUserFollowersInTweets(String tweetJson)
    {
        try{
            return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();

        }catch(NullPointerException ex)
        {
            return 0;
        }
    }
    
    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()); // setting up key to type string
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()); // settign up value to type string
        
        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("tweets_1");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweets) -> extractUserFollowersInTweets(jsonTweets) > 10000 
                //filter for tweets which a user of over 10000 followers
        );
        filteredStream.to("important_tweets");
        
        // build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        
        
        
        //start our streams app
        kafkaStreams.start();
    }
}
