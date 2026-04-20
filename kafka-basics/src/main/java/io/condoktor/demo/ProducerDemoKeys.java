package io.condoktor.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);
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
        for(int i = 0 ; i < 2 ; i++) {
            for (int j = 0; j < 10; j++) {

                String topic = "demo_java";
                String key = "id_" + 1;
                String value = "hello world " + j;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executed every time a record is successfully send and exception is thrown
                        if (e == null) {
                            //Record was successfully sent
                            log.info("key :" + key + " | partition :" + recordMetadata.partition());
                        } else {
                            log.error("Error while  producing ", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //create a producer record
//        ProducerRecord<String,String > producerRecord = new ProducerRecord<>("demo_java","hello world");
//
//        //send data
//        producer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                //executed every time a record is successfully send and exception is thrown
//                if(e==null){
//                    //Record was successfully sent
//                    log.info("Recieved new metadata \n"+
//                            "Topic :" + recordMetadata.topic() + "\n" +
//                            "partition :" + recordMetadata.partition() + "\n" +
//                            "offset :" + recordMetadata.offset() + "\n" +
//                            "timeStamp:" + recordMetadata.timestamp() + "\n"
//                    );
//                } else {
//                    log.error("Error while  producing ",e);
//                }
//            }
//        });

        //flush producer
        producer.flush();

        //close producer
        producer.close();



    }
}
