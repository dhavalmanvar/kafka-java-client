package com.enjoy.dh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.concurrent.CountDownLatch;

public class ConsumerThreadDemo {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "fourth-app";


        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread consumerThread = new ConsumerThread(bootstrapServers, groupId, topic, latch);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down consumer thread");
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        Thread thread = new Thread(consumerThread);
        thread.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            logger.info("Closing application...");
        }
    }

    public static class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch) {
            this.latch = latch;

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            consumer = new KafkaConsumer<String, String>(props);

            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset() + ", Timestamp: " + record.timestamp());
                    }
                }

            } catch (WakeupException e) {
                logger.info("Got wakeup call. Shutdown consumer thread.");
            } finally {
                consumer.close();
                latch.countDown();
            }


        }

        public void shutdown() {
            consumer.wakeup();
        }
    }


}
