/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.kafka_sample.kafka.beginner.course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author microchel
 */
public class ConsumerDemo {
    public static void main(String[] args) {
        
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my_fourth_app";
        String topic = "first_topic";
        
        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties); 
        
        // subscribe consumer to our topics
        //consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));
        
        // poll for new data
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));  
            for(ConsumerRecord<String, String> record : records)
            {
                logger.info("\nKey:" + record.key() + "\nValue: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }
        }
    }
}
