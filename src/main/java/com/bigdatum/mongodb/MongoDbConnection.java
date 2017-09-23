package com.bigdatum.mongodb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDbConnection {
	public static JavaSparkContext createJavaSparkContext(final String[] args) {
		String uri = getMongoClientURI(args);
        dropDatabase(uri);
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.app.id", "MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", uri)
                .set("spark.mongodb.output.uri", uri);

        return new JavaSparkContext(conf);
    }
	 private static String getMongoClientURI(final String[] args) {
	        String uri;
	        if (args.length == 0) {
	            uri = "mongodb://localhost/test"; // default
	        } else {
	            uri = args[0];
	        }
	        return uri;
	    }

	    @SuppressWarnings("resource")
		private static void dropDatabase(final String connectionString) {
	        MongoClientURI uri = new MongoClientURI(connectionString);
	        new MongoClient(uri).dropDatabase(uri.getDatabase());
	    }
}
