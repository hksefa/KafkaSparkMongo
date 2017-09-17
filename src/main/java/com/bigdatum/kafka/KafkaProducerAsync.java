package com.bigdatum.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.bigdatum.data.*;

public class KafkaProducerAsync {
	private static final String topic = "mytopic";
	
	public static void main(String[] args) throws InterruptedException{
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", StringSerializer.class);
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		for (int i=0 ; i < 100; i++){
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "key-" + i, new jsonData().getData());
		//	System.out.println(record.toString());
			producer.send(record);
			Thread.sleep(250);
		}
		producer.close();
	}

}
