package io.conduktor.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoCooperativeMode {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperativeMode.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World - Consumer Demo with Cooperative Mode");

        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-second-app-group";
        String topic = "demo_kafka_java";

        //creating Consumer config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //Create consumer to receive messages
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get reference of current thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, lets exit calling consumer.wakeup()..");
                consumer.wakeup();

                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
        try{
            //Subscribing consumer to Topics (list) - Use Arrays.asList() if its list of topics (multiple)
            consumer.subscribe(Collections.singletonList(topic));

            //poll for new data (from kafka)
            while(true){
                // log.info("Polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record: records){
                    log.info("Key: " + record.key() + ", value:" + record.value()
                            + ", offset:" + record.offset() + ", partition:" + record.partition() + "\n");
                }
            }
        } catch (WakeupException w){
            log.info("Wake up exception!");
            //we ignore this exception as we expected wakeup exception as we manually exit the program
        } catch (Exception e){
            //Unknown exception due to some reason
            log.info("Unknown exception:" + e.getMessage());
        } finally {
            log.info("Lets close the consumer, gracefully!!");
            consumer.close();
        }
    }
}
