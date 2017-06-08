package com.zqykj.bigdata.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "wf-vm:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        while (true) {

        }
    }

    public void send(String key, String value) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(new ProducerRecord<String, String>(topic
                            , key
                            , value)
                    , new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                        }
                    });
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<>(topic,
                        key,
                        value)).get();
                System.out.println("Sent message: (" + key + ", " + value + ")");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
