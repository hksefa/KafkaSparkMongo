package com.bigdatum.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


import scala.Tuple2;

public class SparkNetworkWC {
	
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws InterruptedException{
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[2]");
	    @SuppressWarnings("resource")
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER);
	    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	    JavaPairDStream<String, Integer> wordcount = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
		wordcount.print();
		ssc.start();
		ssc.awaitTermination();
	}

}
