package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class.getName());

    public static void main(String [] args) {


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_assign");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // main Thread
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record: consumerRecords) {
                    logger.info("topic: {}, record key: {}, record value: {}, partition: {}",
                            record.topic(), record.key(), record.value(), record.partition());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }



    }

}
