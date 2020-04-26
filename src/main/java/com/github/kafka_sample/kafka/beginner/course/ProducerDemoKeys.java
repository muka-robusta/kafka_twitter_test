/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.kafka_sample.kafka.beginner.course;
import java.util.Properties;
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
public class ProducerDemoKeys {
    public static void main(String[] args) {
        
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServer = "127.0.0.1:9092";
                 
        // create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        for (int i = 0; i < 10; i++)
        {
            //create a producer record
            String topic = "first_topic";
            String value = "Hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key: " + key);
            // send data - async
            producer.send(record, new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        // success
                        logger.info("\n\nRecieved new metadata: \n" 
                                + "Topic: " + metadata.topic() + "\n" 
                                + "Partition: " + metadata.partition() + "\n" 
                                + "Offsets: " + metadata.offset() + "\n"  
                                + "Timestamp: " + metadata.timestamp());

                    } else {
                        logger.error("Error while producing", exception);
                    }
                }

            });
        }
        // flush n close
        producer.flush();
        producer.close();
    }
}
