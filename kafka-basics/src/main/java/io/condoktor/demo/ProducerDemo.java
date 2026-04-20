package io.condoktor.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {

        log.info("I am kafka producer !");

        // create producer properties
        Properties properties = new Properties();

        //connect the producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String,String > producerRecord = new ProducerRecord<>("demo_java","hello world");

        //send data
        producer.send(producerRecord);

        //flush producer
        producer.flush();

        //close producer
        producer.close();



    }
}
