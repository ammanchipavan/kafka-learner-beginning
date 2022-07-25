package io.conduktor.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Hello World -  Kafka Producer Demo Callback");

        //creating Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i < 10; i++) {

            //Creating Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_kafka_java", "Hello World, This is Java Producer: " + i);

            //Send the data to Kafka (asynchronous)
            producer.send(producerRecord, (metadata, exception) -> {
                // executes everytime message sent successfully or throws exception
                if (exception == null) {
                    log.info("Received new Metadata/ \n"
                            + "Topic: " + metadata.topic() + "\n"
                            + "Partition: " + metadata.partition() + "\n"
                            + "Offset: " + metadata.offset() + "\n"
                            + "Timestamp: " + metadata.timestamp());
                } else {
                    log.error("Error while producing: " + exception);
                }
            });

            Thread.sleep(1000);
        }
        //flush and close the Producer (asynchronous)
        producer.flush();
        producer.close();

    }
}
