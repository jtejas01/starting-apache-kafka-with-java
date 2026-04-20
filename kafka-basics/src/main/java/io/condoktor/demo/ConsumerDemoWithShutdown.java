package io.condoktor.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);
    public static void main(String[] args) {

        log.info("I am kafka consumer!");

        // create producer properties
        Properties properties = new Properties();

        //connect to the Localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        //set the groupId
        String groupId = "my-java-application";
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a refrence of main thread
        final Thread mainThread = Thread.currentThread();

        //adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown,lets exit by calling consumer.wakeup()....");
                consumer.wakeup();

                // join the main thread to all the execution of code to main thread
                try {
                    mainThread.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //poll for data
        try {
            //subcribe to topic
            String topic = "demo_java";
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                log.info("polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key :" + record.key() + "value : " + record.value());
                    log.info("Partition : " + record.partition() + "offset : " + record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        }catch (Exception e){
            log.info("UnExpected exception in consumer ");
        } finally {
            consumer.close(); //close the consumer this will also commit offset
        }
    }
}

