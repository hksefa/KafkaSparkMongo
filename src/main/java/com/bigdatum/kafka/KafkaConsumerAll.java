package com.bigdatum.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerAll {
	private static final String topic = "Carriers";
	private static final String broker = "localhost:9092";
	
	public static void main(String[] args){
		Properties props = new Properties();
		props.put("bootstrap.servers", broker);
		props.put("group.id", "mygroup");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", StringDeserializer.class);
		props.put("auto.offset.reset", "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        boolean running = true;
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
        consumer.close();
		
	}
}
