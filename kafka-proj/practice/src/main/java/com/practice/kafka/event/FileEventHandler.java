package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler{

    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private Boolean sync;
    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topicName, messageEvent.key, messageEvent.value);

        if (this.sync) {
            RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
            logger.info("received : " + recordMetadata.partition() + "//" + recordMetadata.offset());
        } else {
            this.kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info(
                            "partition >> " + metadata.partition() + "\n" +
                            "offset >> " + metadata.offset() + "\n" +
                            "timestamp >> " + metadata.timestamp()
                    );
                } else {
                    logger.error("Error >> " + exception.getMessage());
                }
            });
        }
    }

    public static void main(String[] args) throws Exception{
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkProducer = new KafkaProducer<String, String>(props);
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(kafkProducer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key0001", "this is test message");
        fileEventHandler.onMessage(messageEvent);
    }

}
