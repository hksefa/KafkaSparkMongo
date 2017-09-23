
package com.bigdatum.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.bigdatum.mongodb.MongoDbConnection;
import com.mongodb.spark.config.WriteConfig;

public class kafkaSparkMongo {
	private static final String topic = "mytopic";
	private static final String broker = "localhost:9092";
	private static final String clientid = "Consumer";
	
	public static void main(String[] args) throws InterruptedException{
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkConf conf  = new SparkConf().setMaster("local[2]").setAppName("KafkaSparkMongo");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		new MongoDbConnection();
		JavaSparkContext jsc = MongoDbConnection.createJavaSparkContext(args);
		Collection<String> topics = Arrays.asList(topic);
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,broker);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                        "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                        "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,clientid);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
            
        Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("collection", "stock");
	    writeOverrides.put("writeConcern.w", "majority");
	    @SuppressWarnings("unused")
		WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
	   	
        final JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics,kafkaParams));
		JavaDStream<String> lines = messages.map(ConsumerRecord::value);
		JavaDStream<String> words = lines.map(x->x);
		words.print();
		
		jssc.start();
		jssc.awaitTermination();
	}

}
