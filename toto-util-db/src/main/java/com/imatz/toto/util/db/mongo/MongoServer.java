package com.imatz.toto.util.db.mongo;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * Utility class to manage and provide common utilities in the DB management.
 * 
 * @author C308961
 *
 */
public class MongoServer {

	private MongoClient mongoClient_;

	/**
	 * Default constructor: the MongoDB Host and Port are taken from environment
	 * variables: MONGO_HOST and MONGO_PORT
	 */
	public MongoServer() {

		this(System.getenv("MONGO_HOST"), Integer.parseInt(System.getenv("MONGO_PORT")));

	}

	/**
	 * This constructor takes the host and port of the MongoDB
	 * 
	 * @param host
	 *            the MongoDB Host address
	 * @param port
	 *            the MongoDB Port
	 */
	public MongoServer(String host, int port) {

		Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);
		
		ServerAddress serverAddress = new ServerAddress(host, port);

		mongoClient_ = new MongoClient(serverAddress);
	}

	/**
	 * Returns the specified database
	 * 
	 * @param databaseName
	 *            the name of the database
	 * @return the MongoDatabase wrapper (as a MongoDB object)
	 */
	public MongoDB getDatabase(String databaseName) {

		return new MongoDB(mongoClient_, databaseName);

	}

	/**
	 * Closes the connection with the MongoDB Server
	 */
	public void closeConnection() {

		mongoClient_.close();

	}

}
