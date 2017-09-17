package com.bigdatum.mongodb;



import org.bson.Document;

import com.bigdatum.data.jsonData;
import com.mongodb.MongoClient;


import com.mongodb.client.model.CreateCollectionOptions;

public class TestMongo {
	public static void main(String[] args){
		@SuppressWarnings("resource")
		MongoClient mongoClient = new MongoClient("localhost",27017);
		for (int i=0; i < 100; i++){
			Document doc = Document.parse(new jsonData().getData());
			mongoClient.getDatabase("test").getCollection("cappedCollection").insertOne(doc);
			System.out.println("Inserting: " + doc);
		}
	}
	public static void createCollection(MongoClient mongoClient, String dbase){
		mongoClient.getDatabase("test").createCollection("cappedCollection",
		          new CreateCollectionOptions().capped(true).sizeInBytes(0x100000));
	}
	
	public void insert_data(String collection, String database, String doc){
		@SuppressWarnings("resource")
		MongoClient mongoClient = new MongoClient("localhost",27017);
		Document document = Document.parse(new jsonData().getData());
		mongoClient.getDatabase(database).getCollection(collection).insertOne(document);
		System.out.println("Inserting: " + document);
	}
	
	
}
