package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    /**
     * @throws Exception
     */
    @Override
    public void onOpen() throws Exception {

    }

    /**
     * @throws Exception
     */
    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    /**
     * @param event        the event name, from the {@code event:} line in the stream
     * @param messageEvent a {@link MessageEvent} object containing all the other event properties
     * @throws Exception
     */
    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());

        kafkaProducer.send( new ProducerRecord<>(topic, messageEvent.getData()));
    }

    /**
     * @param comment the comment line
     * @throws Exception
     */
    @Override
    public void onComment(String comment) throws Exception {

    }

    /**
     * @param t a {@code Throwable} object
     */
    @Override
    public void onError(Throwable t) {
        log.error("error in streaming" + t);
    }
}
