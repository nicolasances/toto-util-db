package com.imatz.toto.util.db.mongo;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Class for managing operations on the mongo database
 * 
 * @author C308961
 *
 */
public class MongoDB {

	private MongoDatabase database_;

	public MongoDB(MongoClient mongoClient, String databaseName) {

		database_ = mongoClient.getDatabase(databaseName);

	}

	/**
	 * Wraps the {@link MongoDatabase#getCollection(String)} method providing
	 * the following functionalities:
	 * <ul>
	 * <li>If the specified collection doesn't exist it will create it</li>
	 * </ul>
	 * 
	 * @param collectionName
	 *            the name of the collection
	 * 
	 * @return the collection
	 */
	public MongoCollection<Document> getCollection(String collectionName) {

		return getCollection(collectionName, true);

	}

	/**
	 * Wraps the {@link MongoDatabase#getCollection(String)} method providing
	 * the following functionalities:
	 * <ul>
	 * <li>If the specified collection doesn't exist it will create it if the
	 * createIfNotFound parameter is set to true</li>
	 * </ul>
	 * 
	 * @param collectionName
	 *            the name of the collection
	 * @param createIfNotFound
	 *            true if the collection has to be created in case it hasn't
	 *            been found
	 * @return the collection
	 */
	private MongoCollection<Document> getCollection(String collectionName, boolean createIfNotFound) {

		MongoCollection<Document> collection = database_.getCollection(collectionName);

		if (collection == null && createIfNotFound) {
			
			database_.createCollection(collectionName);

			collection = database_.getCollection(collectionName);
		}

		return collection;

	}

}
